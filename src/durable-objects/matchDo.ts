import type { MatchState, Env, RoundArchive, MatchArchiveSummary } from '../types'; // Ensure types are imported

// Default state for a new match (if not loaded from storage)
const defaultMatchState: Omit<MatchState, 'matchId'> = {
  round: 1,
  teamA_name: 'Team A',
  teamA_score: 0,
  teamA_player: 'Player A1',
  teamB_name: 'Team B',
  teamB_score: 0,
  teamB_player: 'Player B1',
  status: 'pending',
};

export class MatchDO implements DurableObject {
  state: DurableObjectState;
  env: Env; // Store the Env object to access D1, R2 etc.
  matchData: MatchState | null = null;
  matchId: string;
  websockets: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env; // Store env
    this.matchId = state.id.toString(); // Get the DO's own ID

    // Load persisted state if available, otherwise initialize
    this.state.blockConcurrencyWhile(async () => {
      const storedMatchData = await this.state.storage.get<MatchState>('matchData');
      if (storedMatchData) {
        this.matchData = storedMatchData;
      } else {
        // Initialize with default state if nothing is stored
        // This happens when the DO is created for the first time for this ID
        this.matchData = { ...defaultMatchState, matchId: this.matchId };
        // Persist this initial state
        await this.state.storage.put('matchData', this.matchData);
      }
    });
  }

  // Helper to broadcast updates to all connected WebSockets
  private broadcast(message: object | string) {
    const payload = typeof message === 'string' ? message : JSON.stringify(message);
    // Filter out closed connections before broadcasting
    this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
    this.websockets.forEach((ws) => {
      try {
        ws.send(payload);
      } catch (e) {
        console.error('Error sending message to WebSocket:', e);
        // Connection might have closed between filter and send, will be removed on next filter
      }
    });
  }

  // Helper to determine winner based on scores
  private determineWinner(state: { teamA_score: number; teamB_score: number; teamA_name: string; teamB_name: string }): string | null {
      if (state.teamA_score > state.teamB_score) {
          return state.teamA_name || '队伍A'; // Use default name if empty
      } else if (state.teamB_score > state.teamA_score) {
          return state.teamB_name || '队伍B'; // Use default name if empty
      } else {
          return null; // Draw or undecided
      }
  }


  // --- New Methods for Round/Match Management ---

  // Archive the current round's data to D1 round_archives table
  private async archiveCurrentRound(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to archive round." };
    }
    // Prevent archiving if the whole match is already archived
    if (this.matchData.status === 'archived_in_d1') {
        return { success: false, message: "Match is already archived, cannot archive rounds." };
    }

    // Determine winner for this round's archive based on current scores
    const winnerName = this.determineWinner(this.matchData);

    try {
      const stmt = this.env.DB.prepare(
        `INSERT INTO round_archives (match_do_id, round_number, team_a_name, team_a_score, team_a_player,
                                     team_b_name, team_b_score, team_b_player, status, archived_at, raw_data, winner_team_name)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(match_do_id, round_number) DO UPDATE SET
            team_a_name = excluded.team_a_name,
            team_a_score = excluded.team_a_score,
            team_a_player = excluded.team_a_player,
            team_b_name = excluded.team_b_name,
            team_b_score = excluded.team_b_score,
            team_b_player = excluded.team_b_player,
            status = excluded.status,
            archived_at = excluded.archived_at,
            raw_data = excluded.raw_data,
            winner_team_name = excluded.winner_team_name` // Update winner on conflict
      );

      const result = await stmt.bind(
        this.matchData.matchId,
        this.matchData.round,
        this.matchData.teamA_name,
        this.matchData.teamA_score,
        this.matchData.teamA_player,
        this.matchData.teamB_name,
        this.matchData.teamB_score,
        this.matchData.teamB_player,
        this.matchData.status, // Archive the status at the end of the round
        new Date().toISOString(),
        JSON.stringify(this.matchData), // Store raw data
        winnerName // Bind the winner name
      ).run();

      if (result.success) {
        console.log(`DO (${this.matchId}) Round ${this.matchData.round} data archived/updated in D1 round_archives.`);
        return { success: true, message: `Round ${this.matchData.round} archived.`, d1RecordId: result.meta.last_row_id };
      } else {
        console.error(`DO (${this.matchId}) failed to archive round ${this.matchData.round} to D1:`, result.error);
        return { success: false, message: `Failed to archive round: ${result.error}` };
      }

    } catch (e: any) {
      console.error(`DO (${this.matchId}) exception during D1 round archive:`, e);
      return { success: false, message: `Exception during round archive: ${e.message}` };
    }
  }

  // Advance to the next round
  private async nextRound(): Promise<{ success: boolean; message?: string }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to advance round." };
    }
     if (this.matchData.status === 'archived_in_d1') {
        return { success: false, message: "Match is already archived, cannot advance round." };
    }
     // Optional: Prevent advancing if status is 'finished' - depends on workflow
     // if (this.matchData.status === 'finished') {
     //    return { success: false, message: "Match is finished, cannot advance round. Archive match first." };
     // }


    // Optional: Automatically archive the current round before advancing
    // This ensures the state *before* scores are reset is saved
    const archiveResult = await this.archiveCurrentRound();
    if (!archiveResult.success) {
        console.warn("Failed to auto-archive current round before advancing:", archiveResult.message);
        // Decide if you want to stop here or proceed anyway
        // return { success: false, message: "Failed to auto-archive current round before advancing." };
    }

    this.matchData.round += 1;
    this.matchData.teamA_score = 0; // Reset scores for the new round
    this.matchData.teamB_score = 0;
    this.matchData.status = 'pending'; // Reset status for the new round (or 'live'?)

    try {
      await this.state.storage.put('matchData', this.matchData);
      this.broadcast(this.matchData);
      console.log(`DO (${this.matchId}) advanced to Round ${this.matchData.round}`);
      return { success: true, message: `Advanced to Round ${this.matchData.round}` };
    } catch (e: any) {
      console.error(`DO (${this.matchId}) failed to advance round:`, e);
      return { success: false, message: `Failed to advance round: ${e.message}` };
    }
  }

  // Archive the entire match summary to D1 matches_archive table
  private async archiveMatch(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to archive match." };
    }
    if (this.matchData.status === 'archived_in_d1') { // Prevent re-archiving
        return { success: true, message: "Match already archived.", d1RecordId: this.matchData.matchId };
    }

    // Determine winner for the entire match archive (based on final score)
    const matchWinnerName = this.determineWinner(this.matchData);


    try {
      const stmt = this.env.DB.prepare(
        `INSERT INTO matches_archive (match_do_id, final_round, team_a_name, team_a_score, team_a_player,
                                     team_b_name, team_b_score, team_b_player, status, archived_at, raw_data, winner_team_name)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(match_do_id) DO UPDATE SET
            final_round = excluded.final_round,
            team_a_name = excluded.team_a_name,
            team_a_score = excluded.team_a_score,
            team_a_player = excluded.team_a_player,
            team_b_name = excluded.team_b_name,
            team_b_score = excluded.team_b_score,
            team_b_player = excluded.team_b_player,
            status = excluded.status,
            archived_at = excluded.archived_at,
            raw_data = excluded.raw_data,
            winner_team_name = excluded.winner_team_name` // Update winner on conflict
      );

      const finalMatchState = { ...this.matchData };
      // Ensure status is marked as finished or archived in the D1 record
      const finalStatusForArchive = finalMatchState.status === 'pending' || finalMatchState.status === 'live' || finalMatchState.status === 'paused'
                          ? 'finished' // Mark as finished if archived while active
                          : finalMatchState.status;


      const result = await stmt.bind(
        finalMatchState.matchId,
        finalMatchState.round,
        finalMatchState.teamA_name,
        finalMatchState.teamA_score,
        finalMatchState.teamA_player,
        finalMatchState.teamB_name,
        finalMatchState.teamB_score,
        finalMatchState.teamB_player,
        finalStatusForArchive, // Use the determined final status for the D1 record
        new Date().toISOString(),
        JSON.stringify(finalMatchState), // Store raw data as well
        matchWinnerName // Bind the match winner name
      ).run();

      if (result.success) {
        console.log(`DO (${this.matchId}) match data archived/updated in D1 matches_archive.`);

        // Update DO's internal state to reflect the whole match is archived
        this.matchData.status = 'archived_in_d1'; // Custom status for the DO instance
        await this.state.storage.put('matchData', this.matchData);
        this.broadcast(this.matchData); // Notify clients about the archival status

        // Close WebSockets as the live match is over
        this.websockets.forEach(ws => ws.close(1000, "Match archived and finished."));
        this.websockets = [];

        return { success: true, message: "Match data archived to D1.", d1RecordId: result.meta.last_row_id || this.matchData.matchId };
      } else {
        console.error(`DO (${this.matchId}) failed to archive match to D1:`, result.error);
        return { success: false, message: `Failed to archive match: ${result.error}` };
      }

    } catch (e: any) {
      console.error(`DO (${this.matchId}) exception during D1 match archive:`, e);
      return { success: false, message: `Exception during match archive: ${e.message}` };
    }
  }

  // Start a new match by resetting the DO state
  private async newMatch(): Promise<{ success: boolean; message?: string }> {
     // Only allow starting a new match if the current one is archived
     if (this.matchData?.status !== 'archived_in_d1') {
         return { success: false, message: "Current match must be archived before starting a new one." };
     }

    try {
      // Clear all state stored for this DO instance
      await this.state.storage.deleteAll();
      console.log(`DO (${this.matchId}) storage cleared.`);

      // Initialize with default state for the new match
      this.matchData = { ...defaultMatchState, matchId: this.matchId };
      await this.state.storage.put('matchData', this.matchData);
      console.log(`DO (${this.matchId}) initialized for new match.`);

      // Broadcast the new state (clients will see a reset)
      this.broadcast(this.matchData);

      // Note: WebSockets were closed during archiveMatch. Clients will need to reconnect.

      return { success: true, message: "New match started." };
    } catch (e: any) {
      console.error(`DO (${this.matchId}) failed to start new match:`, e);
      return { success: false, message: `Failed to start new match: ${e.message}` };
    }
  }


  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Ensure matchData is loaded (it should be by the constructor's blockConcurrencyWhile)
    // This check is a safeguard, constructor should handle initial load
    if (!this.matchData) {
         console.warn(`DO (${this.matchId}): matchData not loaded before fetch. Attempting load.`);
         await this.state.blockConcurrencyWhile(async () => {
            const storedMatchData = await this.state.storage.get<MatchState>('matchData');
            if (storedMatchData) this.matchData = storedMatchData;
            else this.matchData = { ...defaultMatchState, matchId: this.matchId }; // Fallback
        });
        if (!this.matchData) { // Still null, critical error
             console.error(`DO (${this.matchId}): Failed to initialize matchData.`);
             return new Response(JSON.stringify({ error: "Match data not initialized in DO" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // WebSocket upgrade
    if (url.pathname === '/websocket') { // Internal path for WS
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
      }
      const [client, server] = Object.values(new WebSocketPair());
      this.websockets.push(server);
      server.accept();
      // Send current state immediately upon connection
      if (this.matchData) {
        server.send(JSON.stringify(this.matchData));
      }
      // Handle messages from this specific client (optional, e.g., for pings)
      server.addEventListener('message', event => {
        console.log(`DO (${this.matchId}) WS message from client:`, event.data);
        // server.send(`Echo: ${event.data}`);
      });
      server.addEventListener('close', () => {
        console.log(`DO (${this.matchId}) WebSocket closed.`);
        this.websockets = this.websockets.filter(ws => ws !== server);
      });
      server.addEventListener('error', (err) => {
        console.error(`DO (${this.matchId}) WebSocket error:`, err);
        this.websockets = this.websockets.filter(ws => ws !== server);
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    // Get current state
    if (url.pathname === '/state') { // Internal path
      return new Response(JSON.stringify(this.matchData), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Update match state (only allowed if not archived)
    if (url.pathname === '/update' && request.method === 'POST') { // Internal path
      if (this.matchData?.status === 'archived_in_d1') {
        return new Response(JSON.stringify({ error: "Cannot update an archived match." }), { status: 403, headers: { 'Content-Type': 'application/json' } });
      }
      try {
        const updates = await request.json<Partial<Omit<MatchState, 'matchId'>>>();
        // Prevent updating round or status to archived_in_d1 via this endpoint
        if (updates.round !== undefined && updates.round !== this.matchData.round) {
             // Decide if you allow round changes via update or only via next-round
             // For now, let's disallow changing round directly via update
             delete updates.round;
        }
         if (updates.status === 'archived_in_d1') {
             delete updates.status; // Status to archived_in_d1 only via archive-match
         }


        this.matchData = { ...this.matchData!, ...updates, matchId: this.matchId };
        await this.state.storage.put('matchData', this.matchData);
        this.broadcast(this.matchData);
        return new Response(JSON.stringify({ success: true, data: this.matchData }), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (e: any) {
        console.error(`DO (${this.matchId}) Error processing update payload:`, e);
        return new Response(JSON.stringify({ error: 'Invalid update payload', details: e.message }), { status: 400 });
      }
    }

    // --- Internal Endpoints for Actions ---

    // Internal endpoint to archive current round data to D1
    if (url.pathname === '/internal/archive-round' && request.method === 'POST') {
      const archiveResult = await this.archiveCurrentRound();
      if (archiveResult.success) {
        return new Response(JSON.stringify(archiveResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(archiveResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

    // Internal endpoint to advance to the next round
    if (url.pathname === '/internal/next-round' && request.method === 'POST') {
       const nextRoundResult = await this.nextRound();
       if (nextRoundResult.success) {
           return new Response(JSON.stringify(nextRoundResult), { headers: { 'Content-Type': 'application/json' } });
       } else {
           return new Response(JSON.stringify(nextRoundResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
       }
    }

    // Internal endpoint to archive the entire match to D1
    if (url.pathname === '/internal/archive-match' && request.method === 'POST') {
      const archiveResult = await this.archiveMatch();
      if (archiveResult.success) {
        return new Response(JSON.stringify(archiveResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(archiveResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

     // Internal endpoint to start a new match
    if (url.pathname === '/internal/new-match' && request.method === 'POST') {
      const newMatchResult = await this.newMatch();
      if (newMatchResult.success) {
        return new Response(JSON.stringify(newMatchResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(newMatchResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }


    return new Response('Durable Object: Not found or method not allowed for this path', { status: 404 });
  }
}
