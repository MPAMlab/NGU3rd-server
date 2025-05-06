import type { MatchState, Env, RoundArchive, MatchArchiveSummary, TournamentMatch, Team, Member } from '../types'; // Ensure types are imported

// Default state for a new match (if not loaded from storage) - less relevant now with scheduling
// This is primarily a fallback or for unscheduled matches
const defaultMatchState: Omit<MatchState, 'matchId'> = {
  tournamentMatchId: null, // Default to null
  round: 1,
  teamA_name: 'Team A',
  teamA_score: 0,
  teamA_player: 'Player A1', // Placeholder
  teamB_name: 'Team B',
  teamB_score: 0,
  teamB_player: 'Player B1', // Placeholder
  teamA_members: [], // Default empty
  teamB_members: [], // Default empty
  teamA_player_order_ids: [], // Default empty
  teamB_player_order_ids: [], // Default empty
  current_player_index_a: 0,
  current_player_index_b: 0,
  status: 'pending',
};

export class MatchDO implements DurableObject {
  state: DurableObjectState;
  env: Env;
  matchData: MatchState | null = null;
  matchId: string;
  websockets: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.matchId = state.id.toString();

    this.state.blockConcurrencyWhile(async () => {
      const storedMatchData = await this.state.storage.get<MatchState>('matchData');
      if (storedMatchData) {
        this.matchData = storedMatchData;
        // Ensure new fields are initialized if loading old state without them
        if (this.matchData && this.matchData.teamA_members === undefined) {
             console.warn(`DO (${this.matchId}): Initializing new fields for old state.`);
             this.matchData.tournamentMatchId = null;
             this.matchData.teamA_members = [];
             this.matchData.teamB_members = [];
             this.matchData.teamA_player_order_ids = [];
             this.matchData.teamB_player_order_ids = [];
             this.matchData.current_player_index_a = 0;
             this.matchData.current_player_index_b = 0;
             // Player names might be placeholders from old state, will be updated if initialized from schedule
             await this.state.storage.put('matchData', this.matchData); // Persist updated structure
        }
      } else {
        // DO is being created for the first time for this ID.
        // It should ideally be initialized via /internal/initialize-from-schedule.
        // Initialize with default state as a fallback.
        console.warn(`DO (${this.matchId}): Initializing with default state. Should ideally be initialized from schedule.`);
        this.matchData = { ...defaultMatchState, matchId: this.matchId };
        await this.state.storage.put('matchData', this.matchData);
      }
    });
  }

  // Helper to get player nickname based on member ID and members list
  private getPlayerNickname(memberId: number | undefined, members: Member[] | undefined): string {
      if (memberId === undefined || members === undefined) {
          return '未知选手';
      }
      const member = members.find(m => m.id === memberId);
      return member?.nickname || '未知选手';
  }


  private broadcast(message: object | string) {
    const payload = typeof message === 'string' ? message : JSON.stringify(message);
    this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
    this.websockets.forEach((ws) => {
      try {
        ws.send(payload);
      } catch (e) {
        console.error('Error sending message to WebSocket:', e);
      }
    });
  }

  private determineWinner(state: { teamA_score: number; teamB_score: number; teamA_name: string; teamB_name: string }): string | null {
      if (state.teamA_score > state.teamB_score) {
          return state.teamA_name || '队伍A';
      } else if (state.teamB_score > state.teamA_score) {
          return state.teamB_name || '队伍B';
      } else {
          return null; // Draw or undecided
      }
  }

  // --- New Internal Method: Initialize from Schedule ---
  // Called by the Worker when starting a match from the schedule
  private async initializeFromSchedule(scheduleData: {
      tournamentMatchId: number;
      team1_name: string; // Pass names directly
      team2_name: string;
      team1_members: Member[]; // Pass full member objects
      team2_members: Member[];
      team1_player_order_ids: number[]; // Pass ordered member IDs
      team2_player_order_ids: number[];
  }): Promise<{ success: boolean; message?: string }> {
      console.log(`DO (${this.matchId}): Initializing from schedule for tournament match ${scheduleData.tournamentMatchId}`);

      // Clear existing state if any
      await this.state.storage.deleteAll();

      // Initialize matchData from schedule data
      this.matchData = {
          matchId: this.matchId,
          tournamentMatchId: scheduleData.tournamentMatchId,
          round: 1, // Always start at round 1 for a new live match
          teamA_name: scheduleData.team1_name,
          teamA_score: 0,
          teamA_player: this.getPlayerNickname(scheduleData.team1_player_order_ids[0], scheduleData.team1_members), // Get 1st player nickname based on order
          teamB_name: scheduleData.team2_name,
          teamB_score: 0,
          teamB_player: this.getPlayerNickname(scheduleData.team2_player_order_ids[0], scheduleData.team2_members), // Get 1st player nickname based on order
          teamA_members: scheduleData.team1_members,
          teamB_members: scheduleData.team2_members,
          teamA_player_order_ids: scheduleData.team1_player_order_ids,
          teamB_player_order_ids: scheduleData.team2_player_order_ids,
          current_player_index_a: 0, // Start with the first player in the order
          current_player_index_b: 0, // Start with the first player in the order
          status: 'pending', // Start as pending
      };

      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);
          console.log(`DO (${this.matchId}): State initialized from schedule.`);
          return { success: true, message: "Match initialized from schedule." };
      } catch (e: any) {
          console.error(`DO (${this.matchId}): Failed to save initial state from schedule:`, e);
          return { success: false, message: `Failed to initialize match: ${e.message}` };
      }
  }


  // Archive the current round's data to D1 round_archives table
  private async archiveCurrentRound(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to archive round." };
    }
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
            winner_team_name = excluded.winner_team_name`
      );

      const result = await stmt.bind(
        this.matchData.matchId,
        this.matchData.round,
        this.matchData.teamA_name,
        this.matchData.teamA_score,
        this.matchData.teamA_player, // Archive the player name for this round
        this.matchData.teamB_name,
        this.matchData.teamB_score,
        this.matchData.teamB_player, // Archive the player name for this round
        this.matchData.status,
        new Date().toISOString(),
        JSON.stringify(this.matchData),
        winnerName
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
    const archiveResult = await this.archiveCurrentRound();
    if (!archiveResult.success) {
        console.warn("Failed to auto-archive current round before advancing:", archiveResult.message);
        // Decide if you want to stop here or proceed anyway
        // return { success: false, message: "Failed to auto-archive current round before advancing." };
    }

    this.matchData.round += 1;
    this.matchData.teamA_score = 0; // Reset scores for the new round
    this.matchData.teamB_score = 0;

    // Advance players based on current order
    // Ensure player order arrays are not empty to avoid division by zero
    const teamAOrderLength = this.matchData.teamA_player_order_ids.length || 1;
    const teamBOrderLength = this.matchData.teamB_player_order_ids.length || 1;

    this.matchData.current_player_index_a = (this.matchData.current_player_index_a + 1) % teamAOrderLength;
    this.matchData.current_player_index_b = (this.matchData.current_player_index_b + 1) % teamBOrderLength;

    // Update current player names based on the new index and stored member list
    const currentMemberIdA = this.matchData.teamA_player_order_ids[this.matchData.current_player_index_a];
    const currentMemberIdB = this.matchData.teamB_player_order_ids[this.matchData.current_player_index_b];

    this.matchData.teamA_player = this.getPlayerNickname(currentMemberIdA, this.matchData.teamA_members);
    this.matchData.teamB_player = this.getPlayerNickname(currentMemberIdB, this.matchData.teamB_members);


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
      // Insert/Update into matches_archive
      const archiveStmt = this.env.DB.prepare(
        `INSERT INTO matches_archive (match_do_id, tournament_match_id, final_round, team_a_name, team_a_score, team_a_player,
                                     team_b_name, team_b_score, team_b_player, status, archived_at, raw_data, winner_team_name)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(match_do_id) DO UPDATE SET
            tournament_match_id = excluded.tournament_match_id,
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
            winner_team_name = excluded.winner_team_name`
      );

      const finalMatchState = { ...this.matchData };
      const finalStatusForArchive = finalMatchState.status === 'pending' || finalMatchState.status === 'live' || finalMatchState.status === 'paused'
                          ? 'finished'
                          : finalMatchState.status;


      const archiveResult = await archiveStmt.bind(
        finalMatchState.matchId,
        finalMatchState.tournamentMatchId, // Bind tournament_match_id
        finalMatchState.round,
        finalMatchState.teamA_name,
        finalMatchState.teamA_score,
        finalMatchState.teamA_player,
        finalMatchState.teamB_name,
        finalMatchState.teamB_score,
        finalMatchState.teamB_player,
        finalStatusForArchive,
        new Date().toISOString(),
        JSON.stringify(finalMatchState),
        matchWinnerName
      ).run();

      if (!archiveResult.success) {
           console.error(`DO (${this.matchId}) failed to archive match to D1 matches_archive:`, archiveResult.error);
           // Decide if you want to stop here or proceed with updating tournament_matches
           // For now, let's proceed but return failure if archive failed
           return { success: false, message: `Failed to archive match summary: ${archiveResult.error}` };
      }


      // Update the corresponding tournament_matches entry if it exists
      if (this.matchData.tournamentMatchId) {
          try {
              // Need to get the winner's team_id from the teams table based on winner name
              let winnerTeamId: number | null = null;
              if (matchWinnerName) {
                  // Find the team ID based on the archived team name
                  // Note: This assumes team names are unique or you handle potential duplicates
                  const winnerTeam = await this.env.DB.prepare("SELECT id FROM teams WHERE name = ?").bind(matchWinnerName).first<{ id: number }>();
                  if (winnerTeam) {
                      winnerTeamId = winnerTeam.id;
                  } else {
                      console.warn(`DO (${this.matchId}): Could not find team ID for winner name "${matchWinnerName}" to update tournament_matches.`);
                  }
              }


              const updateTournamentStmt = this.env.DB.prepare(
                  `UPDATE tournament_matches SET
                     status = ?,
                     winner_team_id = ?,
                     match_do_id = ? -- Keep the DO ID linked even after completion
                   WHERE id = ?`
              );
              const updateTournamentResult = await updateTournamentStmt.bind(
                  'completed', // Mark the scheduled match as completed
                  winnerTeamId, // Bind the winner team ID
                  this.matchData.matchId, // Keep the DO ID link
                  this.matchData.tournamentMatchId
              ).run();

              if (!updateTournamentResult.success) {
                  console.error(`DO (${this.matchId}) failed to update tournament_matches entry ${this.matchData.tournamentMatchId}:`, updateTournamentResult.error);
                  // This is a secondary failure, the match summary is archived, but the schedule isn't updated.
                  // Decide how critical this is. For now, log and continue.
              } else {
                   console.log(`DO (${this.matchId}) updated tournament_matches entry ${this.matchData.tournamentMatchId} status to 'completed'.`);
              }

          } catch (e: any) {
              console.error(`DO (${this.matchId}) exception during tournament_matches update:`, e);
              // Log the exception but don't necessarily fail the whole archive operation if matches_archive succeeded
          }
      }


      // Update DO's internal state to reflect the whole match is archived
      this.matchData.status = 'archived_in_d1'; // Custom status for the DO instance
      await this.state.storage.put('matchData', this.matchData);
      this.broadcast(this.matchData); // Notify clients about the archival status

      // Close WebSockets as the live match is over
      this.websockets.forEach(ws => ws.close(1000, "Match archived and finished."));
      this.websockets = [];

      // Return success based on the matches_archive insertion result
      return { success: true, message: "Match data archived to D1.", d1RecordId: archiveResult.meta.last_row_id || this.matchData.matchId };


    } catch (e: any) {
      console.error(`DO (${this.matchId}) exception during D1 match archive (initial insert):`, e);
      return new Response(JSON.stringify({ error: `Exception during match archive: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
  }

  // Start a new match by resetting the DO state
  // This method is now less preferred, initialization should ideally come from schedule
  private async newMatch(): Promise<{ success: boolean; message?: string }> {
     // Only allow starting a new match if the current one is archived
     if (this.matchData?.status !== 'archived_in_d1') {
         return { success: false, message: "Current match must be archived before starting a new one." };
     }

    try {
      // Clear all state stored for this DO instance
      await this.state.storage.deleteAll();
      console.log(`DO (${this.matchId}) storage cleared.`);

      // Initialize with default state for the new match (no associated tournament match)
      this.matchData = { ...defaultMatchState, matchId: this.matchId, tournamentMatchId: null }; // Ensure tournamentMatchId is null
      await this.state.storage.put('matchData', this.matchData);
      console.log(`DO (${this.matchId}) initialized for new match (default).`);

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
        this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
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
        // Allow updating scores, team names (though ideally from schedule), and status (except archived_in_d1)
        const updates = await request.json<Partial<Omit<MatchState, 'matchId' | 'tournamentMatchId' | 'round' | 'teamA_members' | 'teamB_members' | 'teamA_player_order_ids' | 'teamB_player_order_ids' | 'current_player_index_a' | 'current_player_index_b'>>>();

        // Prevent updating core fields via this endpoint
        delete updates.round;
        delete updates.status; // Status changes via specific actions (archive, new-match)
        // Prevent updating player names directly, they are derived
        delete updates.teamA_player;
        delete updates.teamB_player;


        // Apply updates
        this.matchData = { ...this.matchData!, ...updates, matchId: this.matchId };

        // Re-calculate current player names based on potentially updated team names (if allowed)
        // Note: This simple update doesn't allow changing player members or order.
        // If you need to change players/order mid-match, you'd need more complex logic here.
        const currentMemberIdA = this.matchData.teamA_player_order_ids[this.matchData.current_player_index_a];
        const currentMemberIdB = this.matchData.teamB_player_order_ids[this.matchData.current_player_index_b];
        this.matchData.teamA_player = this.getPlayerNickname(currentMemberIdA, this.matchData.teamA_members);
        this.matchData.teamB_player = this.getPlayerNickname(currentMemberIdB, this.matchData.teamB_members);


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

    // New Internal endpoint to initialize DO state from schedule data
    if (url.pathname === '/internal/initialize-from-schedule' && request.method === 'POST') {
        try {
            const scheduleData = await request.json<{
                tournamentMatchId: number;
                team1_name: string;
                team2_name: string;
                team1_members: Member[];
                team2_members: Member[];
                team1_player_order_ids: number[];
                team2_player_order_ids: number[];
            }>();
             if (!scheduleData || scheduleData.tournamentMatchId === undefined || !scheduleData.team1_name || !scheduleData.team2_name || !Array.isArray(scheduleData.team1_members) || !Array.isArray(scheduleData.team2_members) || !Array.isArray(scheduleData.team1_player_order_ids) || !Array.isArray(scheduleData.team2_player_order_ids)) {
                 return new Response(JSON.stringify({ error: "Invalid schedule data payload" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
             }

            const initResult = await this.initializeFromSchedule(scheduleData);
            if (initResult.success) {
                return new Response(JSON.stringify(initResult), { headers: { 'Content-Type': 'application/json' } });
            } else {
                return new Response(JSON.stringify(initResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        } catch (e: any) {
             console.error(`DO (${this.matchId}) Exception processing initialize-from-schedule payload:`, e);
             return new Response(JSON.stringify({ error: 'Invalid initialize-from-schedule payload', details: e.message }), { status: 400 });
        }
    }


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

     // Internal endpoint to start a new match (default, unscheduled)
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
