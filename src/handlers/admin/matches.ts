// src/handlers/admin/matches.ts

import { AuthenticatedRequest, Env, Match, Team, Member, MatchTurn, RecordTurnInput, LiveMatchState, Song } from '../../types';
import { apiResponse, apiError } from '../../index'; // Assuming these are exported from index.ts
import { calculateMatchTurnResult } from '../../utils/damageCalculator'; // Import the calculator

// Load songs data from JSON (assuming it's in src/data/songs.json)
// In a real app, you might load this once on Worker startup or from KV/R2
// For simplicity, let's assume a function to get songs
async function getAllSongs(): Promise<Song[]> {
    // In a real Worker, you'd fetch this from R2, KV, or bundle it
    // For now, let's use a placeholder or assume it's bundled
    // Example: const songsJson = await import('../data/songs.json'); return songsJson.default as Song[];
    console.warn("Placeholder: Using dummy song data. Implement actual song loading.");
    return [
        { id: 1, name: "Song A", artist: "Artist 1", image_url: "", difficulties: { EXP: 12, MST: 13 } },
        { id: 2, name: "Song B", artist: "Artist 2", image_url: "", difficulties: { EXP: 11, MST: 12, ReM: 13 } },
        { id: 3, name: "Song C", artist: "Artist 3", image_url: "", difficulties: { BAS: 7, ADV: 9, EXP: 10 } },
        // Add more dummy songs or load from actual source
    ] as Song[]; // Cast to Song[]
}

// Helper to get a random song (excluding played ones if needed)
async function getRandomSong(env: Env, matchId: number): Promise<Song | null> {
    const allSongs = await getAllSongs(); // Get all songs

    // Fetch songs already played in this match
    const playedSongsResult = await env.DB.prepare('SELECT song_id FROM match_turns WHERE match_id = ? AND song_id IS NOT NULL').bind(matchId).all<{ song_id: number }>();
    const playedSongIds = new Set(playedSongsResult.results?.map(r => r.song_id) || []);

    // Filter out played songs (optional, depending on rules after 12 turns)
    // If after 12 turns, any song can be repeated, skip the filtering.
    // Let's assume for now after 12 turns, any song from the full list is fair game.
    // If it's within the first 12 turns, filter played songs.
    const match = await env.DB.prepare('SELECT current_song_index FROM matches WHERE id = ?').bind(matchId).first<{ current_song_index: number }>();
    const currentTurnIndex = match?.current_song_index ?? 0;

    let availableSongs = allSongs;
    if (currentTurnIndex < 12) { // Assuming first 12 turns use unique songs
        availableSongs = allSongs.filter(song => !playedSongIds.has(song.id));
    }
     console.log(`Found ${allSongs.length} total songs, ${playedSongIds.size} already played. ${availableSongs.length} available for random selection.`);


    if (availableSongs.length === 0) {
        console.warn(`No available songs found for random selection for match ${matchId}.`);
        return null; // No songs left
    }

    // Select a random song from available songs
    const randomIndex = Math.floor(Math.random() * availableSongs.length);
    return availableSongs[randomIndex];
}


// --- Admin Match Handlers ---

// GET /api/admin/matches
export async function handleAdminFetchMatches(request: AuthenticatedRequest, env: Env): Promise<Response> {
    console.log(`Admin user ${request.member?.id} fetching all matches...`);
    // Admin authentication already done by middleware

    try {
        // Fetch all matches, ordered by stage and round number
        const allMatches = await env.DB.prepare(
            'SELECT * FROM matches ORDER BY stage, round_number'
        ).all<Match>();

        // Optionally fetch team names for display
        const matchesWithTeamNames = await Promise.all((allMatches.results || []).map(async match => {
            const teams = await env.DB.batch([
                env.DB.prepare('SELECT name FROM teams WHERE code = ? LIMIT 1').bind(match.team1_code),
                env.DB.prepare('SELECT name FROM teams WHERE code = ? LIMIT 1').bind(match.team2_code),
            ]);
            const team1Name = (teams[0].results?.[0] as { name: string } | undefined)?.name || match.team1_code;
            const team2Name = (teams[1].results?.[0] as { name: string } | undefined)?.name || match.team2_code;
            return {
                ...match,
                team1_name: team1Name,
                team2_name: team2Name,
            };
        }));


        return apiResponse({ matches: matchesWithTeamNames }, 200);
    } catch (e) {
        console.error('Database error fetching all matches for admin:', e);
        return apiError('Failed to fetch all matches.', 500, e);
    }
}

// GET /api/admin/matches/:id
export async function handleAdminGetMatch(request: AuthenticatedRequest, env: Env): Promise<Response> {
    const matchId = parseInt(request.params?.id as string, 10);
    if (isNaN(matchId)) {
        return apiError('Invalid match ID', 400);
    }
    console.log(`Admin user ${request.member?.id} fetching match ${matchId}...`);
    // Admin authentication already done by middleware

    try {
        // Fetch match details, teams, members, and turns in a batch
        const results = await env.DB.batch([
            env.DB.prepare('SELECT * FROM matches WHERE id = ? LIMIT 1').bind(matchId),
            env.DB.prepare('SELECT * FROM teams WHERE code = (SELECT team1_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId),
            env.DB.prepare('SELECT * FROM teams WHERE code = (SELECT team2_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId),
            env.DB.prepare('SELECT * FROM members WHERE team_code = (SELECT team1_code FROM matches WHERE id = ?) ORDER BY joined_at ASC').bind(matchId),
            env.DB.prepare('SELECT * FROM members WHERE team_code = (SELECT team2_code FROM matches WHERE id = ?) ORDER BY joined_at ASC').bind(matchId),
            env.DB.prepare('SELECT * FROM match_turns WHERE match_id = ? ORDER BY song_index ASC').bind(matchId),
        ]);

        const match = results[0].results?.[0] as Match | undefined;
        const team1 = results[1].results?.[0] as Team | undefined;
        const team2 = results[2].results?.[0] as Team | undefined;
        const team1Members = results[3].results as Member[] || [];
        const team2Members = results[4].results as Member[] || [];
        const turns = results[5].results as MatchTurn[] || [];

        if (!match) {
            return apiError(`Match with ID ${matchId} not found.`, 404);
        }
        if (!team1 || !team2) {
             console.error(`Teams not found for match ${matchId}. Team1: ${match.team1_code}, Team2: ${match.team2_code}`);
             return apiError('Teams associated with this match not found.', 500);
        }

        // Combine data into a single response object
        const matchDetails = {
            ...match,
            team1: { ...team1, members: team1Members },
            team2: { ...team2, members: team2Members },
            turns: turns,
        };

        return apiResponse({ match: matchDetails }, 200);

    } catch (e) {
        console.error(`Database error fetching match ${matchId} for admin:`, e);
        return apiError('Failed to fetch match details.', 500, e);
    }
}

// POST /api/admin/matches
export async function handleAdminCreateMatch(request: AuthenticatedRequest, env: Env): Promise<Response> {
    console.log(`Admin user ${request.member?.id} creating a new match...`);
    // Admin authentication already done by middleware

    const body = await request.json().catch(() => null);
    if (!body || typeof body.stage !== 'string' || typeof body.roundNumber !== 'number' || typeof body.team1Code !== 'string' || typeof body.team2Code !== 'string') {
        return apiError('Invalid or missing match details in request body.', 400);
    }

    const { stage, roundNumber, team1Code, team2Code, finalSongs } = body; // finalSongs is optional for 'final' stage

    if (!['prelim', 'semi', 'final'].includes(stage)) return apiError('Invalid match stage.', 400);
    if (team1Code.length !== 4 || isNaN(parseInt(team1Code)) || team2Code.length !== 4 || isNaN(parseInt(team2Code)) || team1Code === team2Code) {
         return apiError('Invalid or identical team codes.', 400);
    }
    if (stage === 'final' && (!Array.isArray(finalSongs) || finalSongs.length !== 3 || finalSongs.some(s => !s.name || !s.difficulty))) {
         // Basic validation for final songs structure
         return apiError('For final stage, exactly 3 songs with name and difficulty are required.', 400);
    }


    try {
        // Verify teams exist
        const teamCheck = await env.DB.batch([
            env.DB.prepare('SELECT 1 FROM teams WHERE code = ? LIMIT 1').bind(team1Code),
            env.DB.prepare('SELECT 1 FROM teams WHERE code = ? LIMIT 1').bind(team2Code),
        ]);

        if (!teamCheck[0].results?.[0] || !teamCheck[1].results?.[0]) {
            return apiError('One or both team codes not found.', 404);
        }

        // Optional: Check for existing match with same teams/stage/round? Depends on rules.

        const now = Math.floor(Date.now() / 1000);

        // Insert the new match
        const insertMatchResult = await env.DB.prepare(
            'INSERT INTO matches (stage, round_number, team1_code, team2_code, status, current_song_index, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        )
        .bind(stage, roundNumber, team1Code, team2Code, 'scheduled', 0, now, now) // Status starts as 'scheduled', index 0
        .run();

        if (!insertMatchResult.success) {
            console.error('Create match database insert failed:', insertMatchResult.error);
            return apiError('Failed to create match due to a database issue.', 500);
        }

        const newMatchId = insertMatchResult.meta.last_row_id;

        // If it's a final match, insert the pre-defined songs into match_turns with placeholder data
        if (stage === 'final' && Array.isArray(finalSongs)) {
             const songInserts: D1PreparedStatement[] = [];
             finalSongs.forEach((song, index) => {
                 songInserts.push(
                     env.DB.prepare(`
                         INSERT INTO match_turns (
                             match_id, song_index, song_id, song_name_override, difficulty_level_played,
                             playing_member_id_team1, playing_member_id_team2, -- These will be null initially
                             score_percent_team1, score_percent_team2, -- These will be null initially
                             calculated_damage_team1, calculated_damage_team2,
                             health_change_team1, health_change_team2,
                             team1_health_before, team2_health_before,
                             team1_health_after, team2_health_after,
                             team1_revive_used_this_turn, team2_revive_used_this_turn,
                             recorded_by_staff_id, recorded_at, calculation_log
                         ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?)
                     `).bind(
                         newMatchId,
                         index, // Use index 0, 1, 2 for the first three turns
                         null, // song_id is null if not from the main song library
                         song.name,
                         song.difficulty,
                         request.member!.id, // Record who created it, even if turn data is null
                         now
                     )
                 );
             });
             // Execute song inserts in a batch (within the same transaction if possible, or separately)
             // For simplicity here, doing it separately after match creation.
             // A single transaction for both match and initial turns would be safer.
             try {
                 await env.DB.batch(songInserts);
                 console.log(`Inserted ${songInserts.length} initial songs for final match ${newMatchId}.`);
             } catch (songInsertError) {
                 console.error(`Failed to insert initial songs for final match ${newMatchId}:`, songInsertError);
                 // Decide how to handle this - maybe delete the match? Or just log and continue?
                 // For now, log and continue. The match exists, but turns might be missing.
             }
        }


        // Fetch the created match to return
        const createdMatch = await env.DB.prepare('SELECT * FROM matches WHERE id = ?').bind(newMatchId).first<Match>();

        return apiResponse({ success: true, message: "Match created successfully.", match: createdMatch }, 201);

    } catch (e) {
        console.error('Error creating match:', e);
        return apiError('Failed to create match.', 500, e);
    }
}

// POST /api/admin/matches/:id/start
export async function handleAdminStartMatch(request: AuthenticatedRequest, env: Env, ctx: ExecutionContext): Promise<Response> {
    const matchId = parseInt(request.params?.id as string, 10);
    if (isNaN(matchId)) {
        return apiError('Invalid match ID', 400);
    }
    console.log(`Admin user ${request.member?.id} starting match ${matchId}...`);
    // Admin authentication already done by middleware

    try {
        // Fetch match and teams
        const results = await env.DB.batch([
             env.DB.prepare('SELECT * FROM matches WHERE id = ? LIMIT 1').bind(matchId),
             env.DB.prepare('SELECT * FROM teams WHERE code = (SELECT team1_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId),
             env.DB.prepare('SELECT * FROM teams WHERE code = (SELECT team2_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId),
             env.DB.prepare('SELECT name FROM teams WHERE code = (SELECT team1_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId), // For live state
             env.DB.prepare('SELECT name FROM teams WHERE code = (SELECT team2_code FROM matches WHERE id = ?) LIMIT 1').bind(matchId), // For live state
        ]);

        const match = results[0].results?.[0] as Match | undefined;
        const team1 = results[1].results?.[0] as Team | undefined;
        const team2 = results[2].results?.[0] as Team | undefined;
        const team1Name = (results[3].results?.[0] as { name: string } | undefined)?.name || team1?.code || 'Team 1';
        const team2Name = (results[4].results?.[0] as { name: string } | undefined)?.name || team2?.code || 'Team 2';


        if (!match) return apiError(`Match with ID ${matchId} not found.`, 404);
        if (!team1 || !team2) return apiError('Teams associated with this match not found.', 500);
        if (match.status !== 'scheduled') return apiError(`Match ${matchId} is already ${match.status}. Cannot start.`, 409);

        const now = Math.floor(Date.now() / 1000);

        // Update match status to 'active'
        const updateMatchResult = await env.DB.prepare(
            'UPDATE matches SET status = ?, updated_at = ? WHERE id = ?'
        )
        .bind('active', now, matchId)
        .run();

        if (!updateMatchResult.success) {
            console.error(`Start match database update failed for match ${matchId}:`, updateMatchResult.error);
            return apiError('Failed to start match due to a database issue.', 500);
        }

        // Initialize Team health and mirror if they are not already set (e.g., from previous rounds in semi/final)
        // Assuming initial health is 100 and mirror is 1 if not set
        if (team1.current_health === null || team1.current_health === undefined) team1.current_health = 100;
        if (team1.has_revive_mirror === null || team1.has_revive_mirror === undefined) team1.has_revive_mirror = 1;
        if (team2.current_health === null || team2.current_health === undefined) team2.current_health = 100;
        if (team2.has_revive_mirror === null || team2.has_revive_mirror === undefined) team2.has_revive_mirror = 1;

         const updateTeamsResult = await env.DB.batch([
             env.DB.prepare('UPDATE teams SET current_health = ?, has_revive_mirror = ?, updated_at = ? WHERE code = ?')
                 .bind(team1.current_health, team1.has_revive_mirror, now, team1.code),
             env.DB.prepare('UPDATE teams SET current_health = ?, has_revive_mirror = ?, updated_at = ? WHERE code = ?')
                 .bind(team2.current_health, team2.has_revive_mirror, now, team2.code),
         ]);
         if (!updateTeamsResult[0].success || !updateTeamsResult[1].success) {
              console.error(`Start match team health/mirror update failed for match ${matchId}:`, updateTeamsResult);
              // Log error but don't necessarily fail the start operation if match status updated
         }


        // --- Notify Durable Object to Initialize State ---
        const initialLiveState: LiveMatchState = {
            matchId: matchId,
            stage: match.stage,
            roundNumber: match.round_number,
            team1Code: team1.code,
            team2Code: team2.code,
            team1Name: team1Name,
            team2Name: team2Name,
            team1Health: team1.current_health,
            team2Health: team2.current_health,
            team1HasMirror: team1.has_revive_mirror === 1,
            team2HasMirror: team2.has_revive_mirror === 1,
            currentSongIndex: match.current_song_index, // Should be 0 initially
            status: 'active', // Match is now active
            winnerTeamCode: null,
            lastTurnLog: undefined, // No turn played yet
            currentTurnInfo: undefined,
            nextTurnInfo: null, // Will be populated when song is selected
        };

        // Get the Durable Object stub for this match
        const doId = env.LIVE_MATCH_DO.idFromName(matchId.toString());
        const stub = env.LIVE_MATCH_DO.get(doId);

        // Send an internal request to the DO to update its state
        ctx.waitUntil(stub.fetch(new Request(stub.id.toString() + '/update-state', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(initialLiveState),
        })).catch(error => {
            console.error(`Failed to notify DO ${matchId} for initial state update:`, error);
        }));
        // --- End Notify Durable Object ---


        return apiResponse({ success: true, message: "Match started successfully.", matchId: matchId }, 200);

    } catch (e) {
        console.error(`Error starting match ${matchId}:`, e);
        return apiError('Failed to start match.', 500, e);
    }
}

// POST /api/admin/matches/:id/record-turn
// This handler is similar to the previous draft but integrated into the admin API structure
export async function handleAdminRecordTurn(request: AuthenticatedRequest, env: Env, ctx: ExecutionContext): Promise<Response> {
  const matchId = parseInt(request.params?.id as string, 10);
  if (isNaN(matchId)) {
    return apiError('Invalid match ID', 400);
  }
  console.log(`Admin user ${request.member?.id} recording turn for match ${matchId}...`);
  // Admin authentication already done by middleware

  let input: RecordTurnInput;
  try {
    input = await request.json() as RecordTurnInput;
    // Basic validation
    if (!input.team1MemberId || !input.team2MemberId || !input.scorePercent1 || !input.scorePercent2 || !input.difficultyLevelPlayed || !input.songName) {
        return apiError('Missing required fields in request body', 400);
    }
  } catch (error) {
    return apiError('Invalid JSON body', 400);
  }

  // Use a transaction for atomicity
  // Fetch data needed for calculation AND for the live state update
  const result = await env.DB.batch([
      // 1. Fetch Match, Teams, and Members data
      env.DB.prepare("SELECT * FROM matches WHERE id = ? LIMIT 1").bind(matchId),
      env.DB.prepare("SELECT * FROM teams WHERE code = (SELECT team1_code FROM matches WHERE id = ?) LIMIT 1").bind(matchId),
      env.DB.prepare("SELECT * FROM teams WHERE code = (SELECT team2_code FROM matches WHERE id = ?) LIMIT 1").bind(matchId),
      env.DB.prepare("SELECT id, team_code, profession, nickname, maimai_id FROM members WHERE id = ? LIMIT 1").bind(input.team1MemberId), // Select needed fields
      env.DB.prepare("SELECT id, team_code, profession, nickname, maimai_id FROM members WHERE id = ? LIMIT 1").bind(input.team2MemberId), // Select needed fields
      env.DB.prepare("SELECT name FROM teams WHERE code = (SELECT team1_code FROM matches WHERE id = ?) LIMIT 1").bind(matchId), // For live state
      env.DB.prepare("SELECT name FROM teams WHERE code = (SELECT team2_code FROM matches WHERE id = ?) LIMIT 1").bind(matchId), // For live state
  ]);

  const match = result[0].results?.[0] as Match | undefined;
  const team1 = result[1].results?.[0] as Team | undefined;
  const team2 = result[2].results?.[0] as Team | undefined;
  const team1PlayingMember = result[3].results?.[0] as Pick<Member, 'id' | 'team_code' | 'profession' | 'nickname' | 'maimai_id'> | undefined;
  const team2PlayingMember = result[4].results?.[0] as Pick<Member, 'id' | 'team_code' | 'profession' | 'nickname' | 'maimai_id'> | undefined;
  const team1Name = (result[5].results?.[0] as { name: string } | undefined)?.name || team1?.code || 'Team 1';
  const team2Name = (result[6].results?.[0] as { name: string } | undefined)?.name || team2?.code || 'Team 2';


  if (!match || !team1 || !team2 || !team1PlayingMember || !team2PlayingMember) {
      return apiError('Match, teams, or playing members not found.', 404);
  }

  // Ensure playing members belong to the correct teams in this match
  if (team1PlayingMember.team_code !== team1.code || team2PlayingMember.team_code !== team2.code) {
       return apiError('Playing members do not belong to the teams in this match.', 400);
  }
  // Ensure match is active
  if (match.status !== 'active') {
       return apiError(`Match ${matchId} is not active. Current status: ${match.status}.`, 409);
  }


  // 2. Prepare input for damage calculation
  const calculationInput: DamageCalculationInput = {
      team1Health: team1.current_health,
      team2Health: team2.current_health,
      team1HasMirror: team1.has_revive_mirror === 1,
      team2HasMirror: team2.has_revive_mirror === 1,
      scorePercent1: input.scorePercent1,
      scorePercent2: input.scorePercent2,
      team1Profession: team1PlayingMember.profession,
      team2Profession: team2PlayingMember.profession,
  };

  // 3. Perform damage calculation
  const calculationResult = calculateMatchTurnResult(calculationInput);

  // 4. Prepare database updates and inserts within the transaction
  const updates: D1PreparedStatement[] = [];
  const now = Math.floor(Date.now() / 1000);
  const nextSongIndex = match.current_song_index + 1;

  // Update Team 1 health and mirror status
  updates.push(
      env.DB.prepare("UPDATE teams SET current_health = ?, has_revive_mirror = ?, updated_at = ? WHERE code = ?")
          .bind(calculationResult.team1HealthAfter, calculationResult.team1MirrorUsedThisTurn ? 0 : team1.has_revive_mirror, now, team1.code)
  );

  // Update Team 2 health and mirror status
   updates.push(
      env.DB.prepare("UPDATE teams SET current_health = ?, has_revive_mirror = ?, updated_at = ? WHERE code = ?")
          .bind(calculationResult.team2HealthAfter, calculationResult.team2MirrorUsedThisTurn ? 0 : team2.has_revive_mirror, now, team2.code)
  );

  // Insert new match_turns record
  updates.push(
      env.DB.prepare(`
          INSERT INTO match_turns (
              match_id, song_index, song_id, song_name_override, difficulty_level_played,
              playing_member_id_team1, playing_member_id_team2,
              score_percent_team1, score_percent_team2,
              calculated_damage_team1, calculated_damage_team2,
              health_change_team1, health_change_team2,
              team1_health_before, team2_health_before,
              team1_health_after, team2_health_after,
              team1_revive_used_this_turn, team2_revive_used_this_turn,
              recorded_by_staff_id, recorded_at, calculation_log
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
          matchId,
          nextSongIndex, // Increment song index for this turn
          input.songId || null, // song_id is nullable
          input.songName,
          input.difficultyLevelPlayed,
          input.team1MemberId,
          input.team2MemberId,
          input.scorePercent1,
          input.scorePercent2,
          calculationResult.team1DamageDealt, // Storing damage dealt before negation
          calculationResult.team2DamageDealt, // Storing damage dealt before negation
          calculationResult.team1HealthChange,
          calculationResult.team2HealthChange,
          team1.current_health, // Health before this turn
          team2.current_health, // Health before this turn
          calculationResult.team1HealthAfter, // Health after this turn
          calculationResult.team2HealthAfter, // Health after this turn
          calculationResult.team1MirrorUsedThisTurn ? 1 : 0,
          calculationResult.team2MirrorUsedThisTurn ? 1 : 0,
          request.member!.id, // Staff member ID
          now, // Current timestamp
          JSON.stringify(calculationResult.log) // Store the calculation log
      )
  );

  // Update match's current song index and status if ended
  let newMatchStatus = match.status;
  let winnerTeamCode: string | null = null;

  if (calculationResult.team1HealthAfter <= 0 || calculationResult.team2HealthAfter <= 0) {
      // One or both teams health <= 0. Match ends.
      newMatchStatus = 'completed';
      console.log(`Match ${matchId} ended condition met.`);

      // Determine winner based on health *after* calculation. Higher health wins.
      if (calculationResult.team1HealthAfter > calculationResult.team2HealthAfter) {
          winnerTeamCode = team1.code;
          console.log(`Match ${matchId} ended: Team 1 wins.`);
      } else if (calculationResult.team2HealthAfter > calculationResult.team1HealthAfter) {
           winnerTeamCode = team2.code;
           console.log(`Match ${matchId} ended: Team 2 wins.`);
      } else {
           // Tie in health <= 0. MATLAB gives A priority. Let's follow that or implement a tie-breaker.
           // For now, follow MATLAB: Team 1 wins on tie <= 0.
           winnerTeamCode = team1.code;
           console.log(`Match ${matchId} ended: Team 1 wins (health tied <= 0).`);
      }
       // TODO: Trigger automatic achievements for winner/loser/participants
  } else {
      // Match is still active
      console.log(`Match ${matchId} continues.`);
  }


   updates.push(
      env.DB.prepare("UPDATE matches SET current_song_index = ?, status = ?, winner_team_code = ?, updated_at = ? WHERE id = ?")
          .bind(nextSongIndex, newMatchStatus, winnerTeamCode, now, matchId)
  );


  // 5. Execute the transaction
  try {
      await env.DB.batch(updates);
      console.log(`Match ${matchId} turn ${nextSongIndex} recorded successfully.`);

      // --- Notify Durable Object to Broadcast State ---
      // Fetch the *latest* state from DB after the transaction commits
      // Or construct it from the calculation result and fetched data
      const latestLiveState: LiveMatchState = {
          matchId: matchId,
          stage: match.stage,
          roundNumber: match.round_number,
          team1Code: team1.code,
          team2Code: team2.code,
          team1Name: team1Name,
          team2Name: team2Name,
          team1Health: calculationResult.team1HealthAfter,
          team2Health: calculationResult.team2HealthAfter,
          team1HasMirror: calculationResult.team1MirrorUsedThisTurn ? false : team1.has_revive_mirror === 1, // Use the *new* mirror status
          team2HasMirror: calculationResult.team2MirrorUsedThisTurn ? false : team2.has_revive_mirror === 1, // Use the *new* mirror status
          currentSongIndex: nextSongIndex, // This turn is now completed, so the index is incremented
          status: newMatchStatus,
          winnerTeamCode: winnerTeamCode,
          lastTurnLog: calculationResult.log, // Include log for the last turn
          currentTurnInfo: { // Info about the turn that *just* finished
              songName: input.songName,
              difficulty: input.difficultyLevelPlayed,
              team1PlayerName: team1PlayingMember.nickname,
              team2PlayerName: team2PlayingMember.nickname,
              team1PlayerMaimaiId: team1PlayingMember.maimai_id,
              team2PlayerMaimaiId: team2PlayingMember.maimai_id,
          },
          nextTurnInfo: null, // Will be populated when the next song is selected
      };

      // Get the Durable Object stub for this match
      const doId = env.LIVE_MATCH_DO.idFromName(matchId.toString());
      const stub = env.LIVE_MATCH_DO.get(doId);

      // Send an internal request to the DO to update its state and broadcast
      ctx.waitUntil(stub.fetch(new Request(stub.id.toString() + '/update-state', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(latestLiveState),
      })).catch(error => {
          console.error(`Failed to notify DO ${matchId} for state update:`, error);
      }));
      // --- End Notify Durable Object ---


      // Return the calculation result and updated healths to the Staff client
      return apiResponse({
          success: true,
          matchId: matchId,
          turnIndex: nextSongIndex,
          team1HealthAfter: calculationResult.team1HealthAfter,
          team2HealthAfter: calculationResult.team2HealthAfter,
          team1MirrorUsed: calculationResult.team1MirrorUsedThisTurn,
          team2MirrorUsed: calculationResult.team2MirrorUsedThisTurn,
          matchStatus: newMatchStatus,
          winnerTeamCode: winnerTeamCode,
          log: calculationResult.log, // Include log for debugging/transparency
      }, 200);

  } catch (error: any) {
      console.error('Database transaction or DO notification failed:', error);
      return apiError('Failed to record match turn', 500, error.message);
  }
}

// POST /api/admin/matches/:id/end
export async function handleAdminEndMatch(request: AuthenticatedRequest, env: Env, ctx: ExecutionContext): Promise<Response> {
    const matchId = parseInt(request.params?.id as string, 10);
    if (isNaN(matchId)) {
        return apiError('Invalid match ID', 400);
    }
    console.log(`Admin user ${request.member?.id} manually ending match ${matchId}...`);
    // Admin authentication already done by middleware

    try {
        // Fetch match
        const match = await env.DB.prepare('SELECT * FROM matches WHERE id = ? LIMIT 1').bind(matchId).first<Match>();

        if (!match) return apiError(`Match with ID ${matchId} not found.`, 404);
        if (match.status === 'completed') return apiResponse({ success: true, message: "Match is already completed." }, 200); // Idempotent

        const now = Math.floor(Date.now() / 1000);

        // Determine winner based on current health if not already set
        let winnerTeamCode = match.winner_team_code;
        if (!winnerTeamCode) {
             const teams = await env.DB.batch([
                 env.DB.prepare('SELECT code, current_health FROM teams WHERE code = ? LIMIT 1').bind(match.team1_code),
                 env.DB.prepare('SELECT code, current_health FROM teams WHERE code = ? LIMIT 1').bind(match.team2_code),
             ]);
             const team1 = teams[0].results?.[0] as Pick<Team, 'code' | 'current_health'> | undefined;
             const team2 = teams[1].results?.[0] as Pick<Team, 'code' | 'current_health'> | undefined;

             if (team1 && team2) {
                 if (team1.current_health > team2.current_health) {
                     winnerTeamCode = team1.code;
                 } else if (team2.current_health > team1.current_health) {
                     winnerTeamCode = team2.code;
                 } else {
                     // Tie - default to team1 or implement tie-breaker
                     winnerTeamCode = team1.code;
                 }
                 console.log(`Determined winner for match ${matchId} based on current health: ${winnerTeamCode}`);
             } else {
                 console.warn(`Could not determine winner for match ${matchId} based on health (teams not found).`);
             }
        }


        // Update match status to 'completed' and set winner if determined
        const updateMatchResult = await env.DB.prepare(
            'UPDATE matches SET status = ?, winner_team_code = ?, updated_at = ? WHERE id = ?'
        )
        .bind('completed', winnerTeamCode, now, matchId)
        .run();

        if (!updateMatchResult.success) {
            console.error(`End match database update failed for match ${matchId}:`, updateMatchResult.error);
            return apiError('Failed to end match due to a database issue.', 500);
        }

        // --- Notify Durable Object to Broadcast Final State ---
        // Fetch latest state including final winner
         const latestMatch = await env.DB.prepare('SELECT * FROM matches WHERE id = ? LIMIT 1').bind(matchId).first<Match>();
         const teams = await env.DB.batch([
             env.DB.prepare('SELECT code, name, current_health, has_revive_mirror FROM teams WHERE code = ? LIMIT 1').bind(match.team1_code),
             env.DB.prepare('SELECT code, name, current_health, has_revive_mirror FROM teams WHERE code = ? LIMIT 1').bind(match.team2_code),
         ]);
         const team1 = teams[0].results?.[0] as Pick<Team, 'code' | 'name' | 'current_health' | 'has_revive_mirror'> | undefined;
         const team2 = teams[1].results?.[0] as Pick<Team, 'code' | 'name' | 'current_health' | 'has_revive_mirror'> | undefined;


         if (latestMatch && team1 && team2) {
             const finalLiveState: LiveMatchState = {
                 matchId: matchId,
                 stage: latestMatch.stage,
                 roundNumber: latestMatch.round_number,
                 team1Code: team1.code,
                 team2Code: team2.code,
                 team1Name: team1.name,
                 team2Name: team2.name,
                 team1Health: team1.current_health,
                 team2Health: team2.current_health,
                 team1HasMirror: team1.has_revive_mirror === 1,
                 team2HasMirror: team2.has_revive_mirror === 1,
                 currentSongIndex: latestMatch.current_song_index,
                 status: latestMatch.status, // Should be 'completed'
                 winnerTeamCode: latestMatch.winner_team_code,
                 lastTurnLog: undefined, // No new turn log on manual end
                 currentTurnInfo: undefined,
                 nextTurnInfo: null,
             };

             const doId = env.LIVE_MATCH_DO.idFromName(matchId.toString());
             const stub = env.LIVE_MATCH_DO.get(doId);

             ctx.waitUntil(stub.fetch(new Request(stub.id.toString() + '/update-state', {
                 method: 'POST',
                 headers: { 'Content-Type': 'application/json' },
                 body: JSON.stringify(finalLiveState),
             })).catch(error => {
                 console.error(`Failed to notify DO ${matchId} for final state update:`, error);
             }));
         } else {
             console.error(`Could not fetch latest match/team data to notify DO for match ${matchId} end.`);
         }
        // --- End Notify Durable Object ---


        return apiResponse({ success: true, message: "Match ended successfully.", matchId: matchId, winnerTeamCode: winnerTeamCode }, 200);

    } catch (e) {
        console.error(`Error ending match ${matchId}:`, e);
        return apiError('Failed to end match.', 500, e);
    }
}

// GET /api/admin/matches/:id/random-song
export async function handleAdminGetRandomSong(request: AuthenticatedRequest, env: Env): Promise<Response> {
    const matchId = parseInt(request.params?.id as string, 10);
    if (isNaN(matchId)) {
        return apiError('Invalid match ID', 400);
    }
    console.log(`Admin user ${request.member?.id} getting random song for match ${matchId}...`);
    // Admin authentication already done by middleware

    try {
        const randomSong = await getRandomSong(env, matchId);

        if (!randomSong) {
            return apiError('No songs available for random selection.', 404);
        }

        return apiResponse({ success: true, song: randomSong }, 200);

    } catch (e) {
        console.error(`Error getting random song for match ${matchId}:`, e);
        return apiError('Failed to get random song.', 500, e);
    }
}

// POST /api/admin/matches/:id/set-final-songs
export async function handleAdminSetFinalSongs(request: AuthenticatedRequest, env: Env): Promise<Response> {
    const matchId = parseInt(request.params?.id as string, 10);
    if (isNaN(matchId)) {
        return apiError('Invalid match ID', 400);
    }
    console.log(`Admin user ${request.member?.id} setting final songs for match ${matchId}...`);
    // Admin authentication already done by middleware

    const body = await request.json().catch(() => null);
    if (!body || !Array.isArray(body.songs) || body.songs.length !== 3 || body.songs.some(s => !s.name || !s.difficulty)) {
         return apiError('Request body must contain an array of exactly 3 songs with name and difficulty.', 400);
    }
    const finalSongs = body.songs as { name: string; difficulty: string }[];


    try {
        // Verify match exists and is a final match
        const match = await env.DB.prepare('SELECT id, stage FROM matches WHERE id = ? LIMIT 1').bind(matchId).first<{ id: number, stage: string }>();
        if (!match) return apiError(`Match with ID ${matchId} not found.`, 404);
        if (match.stage !== 'final') return apiError(`Match ${matchId} is not a final match.`, 400);

        const now = Math.floor(Date.now() / 1000);
        const staffMemberId = request.member!.id;

        // Delete existing turns for this match (if any) to replace them
        // This is a simple approach; a more robust one might update existing turns
        await env.DB.prepare('DELETE FROM match_turns WHERE match_id = ?').bind(matchId).run();
        console.log(`Deleted existing turns for match ${matchId}.`);


        // Insert the 3 specified songs as the first 3 turns
        const songInserts: D1PreparedStatement[] = [];
        finalSongs.forEach((song, index) => {
            songInserts.push(
                env.DB.prepare(`
                    INSERT INTO match_turns (
                        match_id, song_index, song_id, song_name_override, difficulty_level_played,
                        playing_member_id_team1, playing_member_id_team2,
                        score_percent_team1, score_percent_team2,
                        calculated_damage_team1, calculated_damage_team2,
                        health_change_team1, health_change_team2,
                        team1_health_before, team2_health_before,
                        team1_health_after, team2_health_after,
                        team1_revive_used_this_turn, team2_revive_used_this_turn,
                        recorded_by_staff_id, recorded_at, calculation_log
                    ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?)
                `).bind(
                    matchId,
                    index, // Use index 0, 1, 2 for the first three turns
                    null, // song_id is null if not from the main song library
                    song.name,
                    song.difficulty,
                    staffMemberId, // Record who set the songs
                    now
                )
            );
        });

        // Execute song inserts
        const insertResults = await env.DB.batch(songInserts);
        if (insertResults.some(r => !r.success)) {
             console.error(`Failed to insert some final songs for match ${matchId}:`, insertResults);
             // Check if all 3 inserted
             if (insertResults.filter(r => r.success).length !== 3) {
                 // If not all inserted, maybe delete the match or inserted turns?
                 // For now, return error.
                 return apiError('Failed to insert all final songs.', 500);
             }
        }
        console.log(`Inserted 3 final songs for match ${matchId}.`);

        // Reset match current_song_index to 0 if it was higher
        // This ensures the first turn played will be index 0
        await env.DB.prepare('UPDATE matches SET current_song_index = 0, updated_at = ? WHERE id = ? AND current_song_index > 0')
            .bind(now, matchId)
            .run();


        return apiResponse({ success: true, message: "Final songs set successfully.", matchId: matchId }, 200);

    } catch (e) {
        console.error(`Error setting final songs for match ${matchId}:`, e);
        return apiError('Failed to set final songs.', 500, e);
    }
}

// TODO: Add other admin handlers as needed (e.g., edit match, delete match, manage songs, manage achievements)
