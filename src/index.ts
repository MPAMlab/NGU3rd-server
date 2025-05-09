// src/index.ts
import type {
    Env,
    Team, // Keep Team type
    Member, // Keep Member type
    TournamentMatch, // Updated TournamentMatch type
    CreateTournamentMatchPayload, // Keep CreateTournamentMatchPayload
    MatchState, // Updated MatchState type
    CalculateRoundPayload, // Updated CalculateRoundPayload
    ResolveDrawPayload, // Keep ResolveDrawPayload
    MatchScheduleData, // Updated MatchScheduleData
    MemberSongPreference, // New MemberSongPreference type
    Song, // New Song type
    SongFromR2, // New SongFromR2 type
    R2SongList, // New R2SongList type
    MatchSong, // New MatchSong type
    SelectTiebreakerSongPayload, // New SelectTiebreakerSongPayload
    RoundSummary // New RoundSummary type
} from './types';
import { MatchDO } from './durable-objects/matchDo'; // Import the DO class

// Helper to get a DO instance by ID string
const getMatchDO = (doIdString: string, env: Env): DurableObjectStub => {
    const id = env.MATCH_DO.idFromString(doIdString);
    return env.MATCH_DO.get(id);
};

// Helper to handle forwarding requests to DOs
const forwardRequestToDO = async (doIdString: string, env: Env, request: Request, internalPath: string, method: string = 'POST', body?: any): Promise<Response> => {
    try {
        const doStub = getMatchDO(doIdString, env);
        // Use a dummy URL with the correct origin/protocol for the DO fetch
        // The actual path is set in internalPath
        const doUrl = new URL(`https://${request.headers.get('Host') || 'dummy-host'}`);
        doUrl.pathname = internalPath;

        const doRequest = new Request(doUrl.toString(), {
            method: method,
            headers: request.headers, // Keep original headers like Authorization, Content-Type
            body: body ? JSON.stringify(body) : request.body, // Stringify body if provided
            redirect: 'follow',
        });

        // Remove headers that might cause issues or are irrelevant for internal DO fetch
        doRequest.headers.delete('Host'); // Cloudflare handles Host for DOs
        // doRequest.headers.delete('Origin'); // Usually not needed for internal paths

        const response = await doStub.fetch(doRequest);
        return response;

    } catch (e: any) {
        console.error(`Worker: Failed to forward request to DO ${doIdString} for path ${internalPath}:`, e);
        // Return a structured error response
        return new Response(JSON.stringify({ success: false, error: `Failed to communicate with match instance: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
};

// Helper to wrap responses in ApiResponse format
const jsonResponse = <T>(data: T, status: number = 200): Response => {
    return new Response(JSON.stringify({ success: true, data }), {
        status,
        headers: { 'Content-Type': 'application/json' },
    });
};

const errorResponse = (error: string, status: number = 500): Response => {
    return new Response(JSON.stringify({ success: false, error }), {
        status,
        headers: { 'Content-Type': 'application/json' },
    });
};


// --- Worker Entry Point ---
export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);
        const method = request.method;
        const pathname = url.pathname;

        // Apply CORS headers to all responses (adjust as needed for your frontend origin)
        const corsHeaders = {
            'Access-Control-Allow-Origin': '*', // Replace with your frontend origin in production
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        };

        // Handle CORS preflight requests
        if (method === 'OPTIONS') {
            return new Response(null, {
                headers: corsHeaders,
            });
        }

        let response: Response;

        // --- API Endpoints (handled by Worker) ---

        // GET /api/teams
        if (method === 'GET' && pathname === '/api/teams') {
            try {
                // Assuming Team type now matches D1 structure with number id
                const { results } = await env.DB.prepare("SELECT * FROM teams").all<Team>();
                response = jsonResponse(results); // Wrap in ApiResponse
            } catch (e: any) {
                console.error("Worker: Failed to list teams:", e);
                response = errorResponse(e.message);
            }
        }
        // GET /api/teams/:id (Using ID now instead of code)
        else if (method === 'GET' && pathname.match(/^\/api\/teams\/\d+$/)) {
             const parts = pathname.split('/');
             const teamId = parseInt(parts[3], 10);
             if (!isNaN(teamId)) {
                 try {
                     const team = await env.DB.prepare("SELECT * FROM teams WHERE id = ?").bind(teamId).first<Team>();
                     if (team) {
                         response = jsonResponse(team);
                     } else {
                         response = errorResponse("Team not found", 404);
                     }
                 } catch (e: any) {
                     console.error(`Worker: Failed to get team ${teamId}:`, e);
                     response = errorResponse(e.message);
                 }
             } else {
                 response = errorResponse("Invalid team ID in path", 400);
             }
        }
        // GET /api/members
        else if (method === 'GET' && pathname === '/api/members') {
            try {
                // Assuming Member type now matches D1 structure with number id and team_code
                const teamCode = url.searchParams.get('team_code'); // Still filter by team_code
                let query = "SELECT * FROM members";
                let params: string[] = [];
                if (teamCode) {
                    query += " WHERE team_code = ?";
                    params.push(teamCode);
                }
                const { results } = await env.DB.prepare(query).bind(...params).all<Member>();
                response = jsonResponse(results); // Wrap in ApiResponse
            } catch (e: any) {
                console.error("Worker: Failed to list members:", e);
                response = errorResponse(e.message);
            }
        }
        // GET /api/members/:id
        else if (method === 'GET' && pathname.match(/^\/api\/members\/\d+$/)) {
             const parts = pathname.split('/');
             const memberId = parseInt(parts[3], 10);
             if (!isNaN(memberId)) {
                 try {
                     const member = await env.DB.prepare("SELECT * FROM members WHERE id = ?").bind(memberId).first<Member>();
                     if (member) {
                         response = jsonResponse(member);
                     } else {
                         response = errorResponse("Member not found", 404);
                     }
                 } catch (e: any) {
                     console.error(`Worker: Failed to get member ${memberId}:`, e);
                     response = errorResponse(e.message);
                 }
             } else {
                 response = errorResponse("Invalid member ID in path", 400);
             }
        }

        // --- New Song Endpoints ---

        // GET /api/songs (Get songs from D1, supports filtering/search)
        else if (method === 'GET' && pathname === '/api/songs') {
            try {
                // Implement filtering/search based on query parameters (e.g., ?category=...&search=...)
                // For simplicity, fetching all for now
                const { results } = await env.DB.prepare("SELECT * FROM songs").all<Song>();

                // Optional: Parse levels_json and construct fullCoverUrl for each song
                const songsWithDetails = results.map(song => {
                    const parsedLevels = song.levels_json ? JSON.parse(song.levels_json) : undefined;
                    const fullCoverUrl = song.cover_filename ? `https://${env.SONG_COVER_BUCKET.name}/${song.cover_filename}` : undefined; // Assuming SONG_COVER_BUCKET binding
                    return { ...song, parsedLevels, fullCoverUrl };
                });

                response = jsonResponse(songsWithDetails);
            } catch (e: any) {
                console.error("Worker: Failed to list songs:", e);
                response = errorResponse(e.message);
            }
        }

        // POST /admin/sync_songs_from_r2 (Admin endpoint to sync songs from R2 JSON)
        // This should ideally be protected by authentication/authorization
        else if (method === 'POST' && pathname === '/admin/sync_songs_from_r2') {
             // TODO: Add authentication/authorization check here

             try {
                 // 1. Fetch JSON from R2
                 const object = await env.SONG_JSON_BUCKET.get(env.SONG_JSON_KEY); // Assuming SONG_JSON_KEY binding
                 if (!object) {
                     return errorResponse(`Song JSON file not found in R2 at key: ${env.SONG_JSON_KEY}`, 404);
                 }
                 const songListData: R2SongList = await object.json();

                 // 2. Check version (optional but recommended)
                 // const currentVersion = await env.DB.prepare("SELECT value FROM settings WHERE key = 'song_data_version'").first<{ value: string }>();
                 // if (currentVersion && currentVersion.value === songListData.data) {
                 //     console.log("Worker: Song data version is the same. Skipping sync.");
                 //     return jsonResponse({ message: "Song data is already up to date." });
                 // }

                 // 3. Clear existing songs table (or perform upsert logic)
                 // Clearing is simpler for initial setup, upsert is better for updates
                 await env.DB.prepare("DELETE FROM songs").run();
                 console.log("Worker: Cleared existing songs table.");

                 // 4. Insert new songs
                 const insertStmt = env.DB.prepare(
                     `INSERT INTO songs (title, category, bpm, levels_json, type, cover_filename, source_data_version)
                      VALUES (?, ?, ?, ?, ?, ?, ?)`
                 );

                 const songsToInsert = songListData.曲目列表.map(song => ({
                     title: song.曲名,
                     category: song.分类,
                     bpm: song.BPM,
                     levels_json: JSON.stringify(song.等级),
                     type: song.类型,
                     cover_filename: song.封面,
                     source_data_version: songListData.data,
                 }));

                 // D1 batch insertion
                 const insertResults = await env.DB.batch(songsToInsert.map(song => insertStmt.bind(
                     song.title, song.category, song.bpm, song.levels_json, song.type, song.cover_filename, song.source_data_version
                 )));

                 // 5. Update version setting (optional)
                 // await env.DB.prepare("REPLACE INTO settings (key, value) VALUES ('song_data_version', ?)").bind(songListData.data).run();

                 console.log(`Worker: Synced ${insertResults.length} songs from R2.`);
                 response = jsonResponse({ success: true, message: `Successfully synced ${insertResults.length} songs.` });

             } catch (e: any) {
                 console.error("Worker: Failed to sync songs from R2:", e);
                 response = errorResponse(`Failed to sync songs: ${e.message}`);
             }
        }


        // --- New Member Song Preference Endpoints ---

        // POST /api/member_song_preferences
        else if (method === 'POST' && pathname === '/api/member_song_preferences') {
             // TODO: Add authentication/authorization check (ensure member_id matches logged-in user or admin)
             try {
                 const payload: MemberSongPreference = await request.json();

                 if (!payload.member_id || !payload.tournament_stage || !payload.song_id || !payload.selected_difficulty) {
                     return errorResponse("Missing required fields for member song preference.", 400);
                 }

                 // Optional: Validate member_id, song_id exist in DB

                 const stmt = env.DB.prepare(
                     `INSERT INTO member_song_preferences (member_id, tournament_stage, song_id, selected_difficulty, created_at)
                      VALUES (?, ?, ?, ?, ?)
                      ON CONFLICT(member_id, tournament_stage, song_id, selected_difficulty) DO UPDATE SET
                          created_at = excluded.created_at -- Simple update on conflict
                     `
                 );

                 const result = await stmt.bind(
                     payload.member_id,
                     payload.tournament_stage,
                     payload.song_id,
                     payload.selected_difficulty,
                     new Date().toISOString()
                 ).run();

                 if (result.success) {
                     // Fetch the inserted/updated preference to return
                     const newPreference = await env.DB.prepare("SELECT * FROM member_song_preferences WHERE id = ?").bind(result.meta.last_row_id).first<MemberSongPreference>();
                     response = jsonResponse(newPreference, 201);
                 } else {
                     console.error("Worker: Failed to save member song preference:", result.error);
                     response = errorResponse(result.error || "Failed to save preference.");
                 }

             } catch (e: any) {
                 console.error("Worker: Exception saving member song preference:", e);
                 response = errorResponse(e.message);
             }
        }

        // GET /api/member_song_preferences?member_id=:id&stage=:stage
        else if (method === 'GET' && pathname === '/api/member_song_preferences') {
             // TODO: Add authentication/authorization check (ensure member_id matches logged-in user or admin)
             try {
                 const memberId = url.searchParams.get('member_id');
                 const stage = url.searchParams.get('stage');

                 if (!memberId || !stage) {
                     return errorResponse("Missing member_id or stage query parameter.", 400);
                 }

                 const memberIdNum = parseInt(memberId, 10);
                 if (isNaN(memberIdNum)) {
                     return errorResponse("Invalid member_id.", 400);
                 }

                 // Join with songs table to get song details
                 const query = `
                     SELECT
                         msp.*,
                         s.title AS song_title,
                         s.cover_filename AS cover_filename,
                         s.levels_json AS levels_json -- Include levels_json to parse difficulty
                     FROM member_song_preferences msp
                     JOIN songs s ON msp.song_id = s.id
                     WHERE msp.member_id = ? AND msp.tournament_stage = ?
                 `;

                 const { results } = await env.DB.prepare(query).bind(memberIdNum, stage).all<MemberSongPreference & { levels_json: string | null }>();

                 // Optional: Parse levels_json and construct fullCoverUrl for each preference
                 const preferencesWithDetails = results.map(pref => {
                     const parsedLevels = pref.levels_json ? JSON.parse(pref.levels_json) : undefined;
                     const fullCoverUrl = pref.cover_filename ? `https://${env.SONG_COVER_BUCKET.name}/${pref.cover_filename}` : undefined; // Assuming SONG_COVER_BUCKET binding
                     // Remove levels_json from the final object if you don't want to expose it directly
                     const { levels_json, ...rest } = pref;
                     return { ...rest, parsedLevels, fullCoverUrl };
                 });


                 response = jsonResponse(preferencesWithDetails);

             } catch (e: any) {
                 console.error("Worker: Failed to get member song preferences:", e);
                 response = errorResponse(e.message);
             }
        }


        // --- Tournament Match Endpoints (Updated) ---

        // GET /api/tournament_matches (Updated to parse JSON fields)
        else if (method === 'GET' && pathname === '/api/tournament_matches') {
            try {
                const query = `
                    SELECT
                        tm.*,
                        t1.code AS team1_code,
                        t1.name AS team1_name,
                        t2.code AS team2_code,
                        t2.name AS team2_name,
                        tw.code AS winner_team_code,
                        tw.name AS winner_team_name
                    FROM tournament_matches tm
                    JOIN teams t1 ON tm.team1_id = t1.id
                    JOIN teams t2 ON tm.team2_id = t2.id
                    LEFT JOIN teams tw ON tm.winner_team_id = tw.id
                    ORDER BY tm.created_at DESC; -- Order by creation date, or scheduled_time
                `;
                const { results } = await env.DB.prepare(query).all<TournamentMatch>();

                // Parse JSON fields before returning
                const matchesWithParsedData = results.map(match => ({
                    ...match,
                    team1_player_order: match.team1_player_order_json ? JSON.parse(match.team1_player_order_json) : null,
                    team2_player_order: match.team2_player_order_json ? JSON.parse(match.team2_player_order_json) : null,
                    match_song_list: match.match_song_list_json ? JSON.parse(match.match_song_list_json) : null,
                    // Remove raw JSON fields if not needed by frontend
                    // team1_player_order_json: undefined,
                    // team2_player_order_json: undefined,
                    // match_song_list_json: undefined,
                }));

                response = jsonResponse(matchesWithParsedData);
            } catch (e: any) {
                console.error("Worker: Failed to list tournament matches:", e);
                response = errorResponse(e.message);
            }
        }

        // POST /api/tournament_matches (Updated for new table structure, no player order/songs yet)
        else if (method === 'POST' && pathname === '/api/tournament_matches') {
            // TODO: Add authentication/authorization check
            try {
                // Use a simplified payload for creation, player order and songs are set in confirm_setup
                interface SimpleCreateTournamentMatchPayload {
                    round_name: string;
                    team1_id: number | null;
                    team2_id: number | null;
                    scheduled_time?: string | null;
                }
                const payload: SimpleCreateTournamentMatchPayload = await request.json();

                if (!payload.round_name || payload.team1_id === null || payload.team2_id === null) {
                     return errorResponse("Missing required fields: round_name, team1_id, team2_id", 400);
                }

                // Basic validation for team IDs
                const team1 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team1_id).first();
                const team2 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team2_id).first();
                if (!team1 || !team2) {
                     return errorResponse("Invalid team1_id or team2_id", 400);
                }

                const stmt = env.DB.prepare(
                    `INSERT INTO tournament_matches (round_name, team1_id, team2_id, scheduled_time, status, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`
                );

                const result = await stmt.bind(
                    payload.round_name,
                    payload.team1_id,
                    payload.team2_id,
                    payload.scheduled_time || null,
                    'scheduled', // New matches start as scheduled
                    new Date().toISOString(),
                    new Date().toISOString()
                ).run();

                if (result.success) {
                    // Fetch the newly created match to return it with its ID and default status
                    const newMatch = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(result.meta.last_row_id).first<TournamentMatch>();
                    response = jsonResponse(newMatch, 201);
                } else {
                    console.error("Worker: Failed to create tournament match:", result.error);
                    response = errorResponse(result.error || "Failed to create match.");
                }

            } catch (e: any) {
                console.error("Worker: Exception creating tournament match:", e);
                response = errorResponse(e.message);
            }
        }

        // PUT /api/tournament_matches/:id/confirm_setup (New endpoint for Staff to finalize setup)
        else if (method === 'PUT' && pathname.match(/^\/api\/tournament_matches\/\d+\/confirm_setup$/)) {
             // TODO: Add authentication/authorization check (Staff only)
             const parts = pathname.split('/');
             const tournamentMatchId = parseInt(parts[3], 10);

             if (!isNaN(tournamentMatchId)) {
                 try {
                     interface ConfirmSetupPayload {
                         team1_player_order: number[];
                         team2_player_order: number[];
                         match_song_list: MatchSong[];
                     }
                     const payload: ConfirmSetupPayload = await request.json();

                     // Basic payload validation
                     if (!Array.isArray(payload.team1_player_order) || !Array.isArray(payload.team2_player_order) || !Array.isArray(payload.match_song_list) || payload.team1_player_order.length === 0 || payload.team2_player_order.length === 0 || payload.match_song_list.length === 0) {
                         return errorResponse("Invalid payload: player orders and song list must be non-empty arrays.", 400);
                     }
                     // TODO: More robust validation: check if player IDs exist and belong to the correct teams, check if song IDs exist.

                     // Fetch the match to ensure it exists and is in a valid state (e.g., 'scheduled' or 'pending_song_confirmation')
                     const match = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<TournamentMatch>();
                     if (!match) {
                         return errorResponse("Tournament match not found.", 404);
                     }
                     if (match.status !== 'scheduled' && match.status !== 'pending_song_confirmation') {
                          return errorResponse(`Match status is '${match.status}'. Must be 'scheduled' or 'pending_song_confirmation' to confirm setup.`, 400);
                     }


                     const stmt = env.DB.prepare(
                         `UPDATE tournament_matches SET
                            team1_player_order_json = ?,
                            team2_player_order_json = ?,
                            match_song_list_json = ?,
                            status = ?,
                            updated_at = ?
                          WHERE id = ?`
                     );

                     const result = await stmt.bind(
                         JSON.stringify(payload.team1_player_order),
                         JSON.stringify(payload.team2_player_order),
                         JSON.stringify(payload.match_song_list),
                         'ready_to_start', // Status changes to ready_to_start
                         new Date().toISOString(),
                         tournamentMatchId
                     ).run();

                     if (result.success) {
                         // Fetch the updated match
                         const updatedMatch = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<TournamentMatch>();
                         response = jsonResponse(updatedMatch);
                     } else {
                         console.error("Worker: Failed to confirm match setup:", result.error);
                         response = errorResponse(result.error || "Failed to confirm setup.");
                     }

                 } catch (e: any) {
                     console.error(`Worker: Exception confirming match setup ${tournamentMatchId}:`, e);
                     response = errorResponse(e.message);
                 }
             } else {
                 response = errorResponse("Invalid tournament match ID", 400);
             }
        }


        // POST /api/tournament_matches/:id/start_live (Updated logic)
        else if (method === 'POST' && pathname.match(/^\/api\/tournament_matches\/\d+\/start_live$/)) {
             // TODO: Add authentication/authorization check (Staff only)
             const parts = pathname.split('/');
             const tournamentMatchId = parseInt(parts[3], 10);

             if (!isNaN(tournamentMatchId)) {
                 try {
                     // 1. Fetch the match details from D1 (must be 'ready_to_start')
                     const match = await env.DB.prepare(
                         `SELECT tm.*, t1.name AS team1_name, t2.name AS team2_name
                          FROM tournament_matches tm
                          JOIN teams t1 ON tm.team1_id = t1.id
                          JOIN teams t2 ON tm.team2_id = t2.id
                          WHERE tm.id = ?`
                     ).bind(tournamentMatchId).first<TournamentMatch>();

                     if (!match) {
                         return errorResponse("Scheduled match not found", 404);
                     }
                     if (match.status === 'live' && match.match_do_id) {
                         // If already live, just return the existing DO ID
                         console.log(`Worker: Match ${tournamentMatchId} is already live with DO ${match.match_do_id}. Returning existing DO ID.`);
                         return jsonResponse({ message: "Match is already live.", match_do_id: match.match_do_id });
                     }
                     if (match.status !== 'ready_to_start') {
                          return errorResponse(`Match status is '${match.status}'. Must be 'ready_to_start' to start live.`, 400);
                     }
                     if (!match.team1_player_order_json || !match.team2_player_order_json || !match.match_song_list_json) {
                          return errorResponse("Match setup is incomplete (player order or song list missing).", 400);
                     }

                     // 2. Fetch member details for both teams
                     // Assuming members table uses team_code, need to get team codes first
                     const team1 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team1_id).first<{ code: string }>();
                     const team2 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team2_id).first<{ code: string }>();
                     if (!team1 || !team2) {
                          return errorResponse("Could not fetch team codes for members.", 500);
                     }

                     const team1Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team1.code).all<Member>();
                     const team2Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team2.code).all<Member>();

                     if (!team1Members.results || team1Members.results.length === 0 || !team2Members.results || team2Members.results.length === 0) {
                          return errorResponse("Could not fetch members for one or both teams.", 500);
                     }

                     // 3. Parse player order and song list JSON
                     const team1PlayerOrderIds: number[] = JSON.parse(match.team1_player_order_json);
                     const team2PlayerOrderIds: number[] = JSON.parse(match.team2_player_order_json);
                     const matchSongList: MatchSong[] = JSON.parse(match.match_song_list_json);

                     // Basic validation for parsed data
                     if (!Array.isArray(team1PlayerOrderIds) || !Array.isArray(team2PlayerOrderIds) || !Array.isArray(matchSongList) || team1PlayerOrderIds.length === 0 || team2PlayerOrderIds.length === 0 || matchSongList.length === 0) {
                          return errorResponse("Parsed setup data is invalid.", 500); // Should not happen if confirm_setup validated
                     }
                     // TODO: More robust validation of parsed IDs/songs against actual members/songs

                     // 4. Generate a DO ID (e.g., based on tournamentMatchId)
                     const doIdString = `match-${tournamentMatchId}`; // Simple mapping

                     // 5. Get the DO instance and prepare initialization payload
                     const doStub = getMatchDO(doIdString, env);
                     const initPayload: MatchScheduleData = {
                         tournamentMatchId: tournamentMatchId,
                         round_name: match.round_name, // Pass round_name
                         team1_id: match.team1_id, // Pass team IDs
                         team2_id: match.team2_id,
                         team1_name: match.team1_name || 'Team A',
                         team2_name: match.team2_name || 'Team B',
                         team1_members: team1Members.results, // Pass full member objects
                         team2_members: team2Members.results,
                         team1_player_order_ids: team1PlayerOrderIds, // Pass ordered IDs
                         team2_player_order_ids: team2PlayerOrderIds,
                         match_song_list: matchSongList, // Pass the song list
                     };

                     // 6. Forward initialization request to the DO
                     const doResponse = await forwardRequestToDO(doIdString, env, request, '/internal/initialize-from-schedule', 'POST', initPayload);

                     if (doResponse.ok) {
                         // 7. Update the tournament_matches record in D1
                         const updateStmt = env.DB.prepare("UPDATE tournament_matches SET status = ?, match_do_id = ?, updated_at = ? WHERE id = ?");
                         const updateResult = await updateStmt.bind('live', doIdString, new Date().toISOString(), tournamentMatchId).run();

                         if (updateResult.success) {
                              console.log(`Worker: Tournament match ${tournamentMatchId} status updated to 'live' with DO ${doIdString}.`);
                              // Return the DO's initialization response (or a simplified success)
                              const doResult = await doResponse.json(); // Assuming DO returns JSON
                              response = jsonResponse({ message: "Match started live.", match_do_id: doIdString, do_init_result: doResult });
                         } else {
                              console.error(`Worker: Failed to update tournament_matches status for ${tournamentMatchId}:`, updateResult.error);
                              // Decide how to handle this - DO is initialized, but schedule not updated.
                              // For now, return error but DO is likely fine.
                              response = errorResponse(`Match started live in DO, but failed to update schedule status: ${updateResult.error}`, 500);
                         }
                     } else {
                         // DO initialization failed
                         const errorBody = await doResponse.json(); // Assuming DO returns JSON error
                         console.error(`Worker: Failed to initialize DO ${doIdString} for match ${tournamentMatchId}:`, errorBody);
                         response = errorResponse(`Failed to initialize live match in Durable Object: ${errorBody.message || errorBody.error}`, doResponse.status);
                     }

                 } catch (e: any) {
                     console.error(`Worker: Exception starting live match ${tournamentMatchId}:`, e);
                     response = errorResponse(`Exception starting live match: ${e.message}`);
                 }
             } else {
                 response = errorResponse("Invalid tournament match ID", 400);
             }
        }

        // --- API Endpoints for Live Matches (forwarded to DO) ---
        // These paths have the structure /api/live-match/:doIdString/...
        else if (pathname.startsWith('/api/live-match/')) {
            const parts = pathname.split('/');
            const doIdString = parts[3]; // e.g., /api/live-match/some-id/state -> parts = ["", "api", "live-match", "some-id", "state"]
            const action = parts[4]; // e.g., "state", "websocket", "calculate_round", etc.

            if (parts.length >= 4 && doIdString) {
                switch (action) {
                    case 'state':
                        if (method === 'GET' && parts.length === 5) {
                            // Forward GET /api/live-match/:doIdString/state to DO's internal /state
                            response = await forwardRequestToDO(doIdString, env, request, '/state', 'GET');
                        } else {
                            response = new Response("Method not allowed for /state", { status: 405 });
                        }
                        break;
                    case 'websocket':
                        if (method === 'GET' && parts.length === 5) {
                             // Forward WebSocket upgrade request to DO's internal /websocket
                             const doStub = getMatchDO(doIdString, env);
                             // The DO's fetch method will handle the 'Upgrade: websocket' header.
                             // Use the original request URL to preserve headers needed for upgrade.
                             const doUrl = new URL(request.url);
                             doUrl.pathname = '/websocket'; // Internal path for WS
                             response = await doStub.fetch(doUrl.toString(), request); // Forward the original request
                        } else {
                            response = new Response("Method not allowed for /websocket", { status: 405 });
                        }
                        break;
                    case 'calculate_round':
                        if (method === 'POST' && parts.length === 5) {
                            try {
                                const payload: CalculateRoundPayload = await request.json();
                                // Basic payload validation (percentages are numbers)
                                if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
                                     return errorResponse("Invalid payload: teamA_percentage and teamB_percentage must be numbers.", 400);
                                }
                                // Forward POST /api/live-match/:doIdString/calculate_round to DO's internal /internal/calculate-round
                                response = await forwardRequestToDO(doIdString, env, request, '/internal/calculate-round', 'POST', payload);
                            } catch (e: any) {
                                console.error(`Worker: Exception processing calculate_round payload for DO ${doIdString}:`, e);
                                response = errorResponse(`Invalid payload format: ${e.message}`, 400);
                            }
                        } else {
                            response = new Response("Method not allowed for /calculate_round", { status: 405 });
                        }
                        break;
                    case 'next_round':
                        if (method === 'POST' && parts.length === 5) {
                            // Forward POST /api/live-match/:doIdString/next_round to DO's internal /internal/next-round
                            response = await forwardRequestToDO(doIdString, env, request, '/internal/next-round', 'POST');
                        } else {
                            response = new Response("Method not allowed for /next_round", { status: 405 });
                        }
                        break;
                    case 'archive':
                         if (method === 'POST' && parts.length === 5) {
                             // Forward POST /api/live-match/:doIdString/archive to DO's internal /internal/archive-match
                             response = await forwardRequestToDO(doIdString, env, request, '/internal/archive-match', 'POST');
                         } else {
                             response = new Response("Method not allowed for /archive", { status: 405 });
                         }
                         break;
                    case 'resolve_draw':
                         if (method === 'POST' && parts.length === 5) {
                             try {
                                 const payload: ResolveDrawPayload = await request.json();
                                 // Basic payload validation
                                 if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
                                      return errorResponse("Invalid payload: winner must be 'teamA' or 'teamB'.", 400);
                                 }
                                 // Forward POST /api/live-match/:doIdString/resolve_draw to DO's internal /internal/resolve-draw
                                 response = await forwardRequestToDO(doIdString, env, request, '/internal/resolve-draw', 'POST', payload);
                             } catch (e: any) {
                                 console.error(`Worker: Exception processing resolve_draw payload for DO ${doIdString}:`, e);
                                 response = errorResponse(`Invalid payload format: ${e.message}`, 400);
                             }
                         } else {
                             response = new Response("Method not allowed for /resolve_draw", { status: 405 });
                         }
                         break;
                    case 'select_tiebreaker_song': // New endpoint for Staff to select tiebreaker
                         if (method === 'POST' && parts.length === 5) {
                             // TODO: Add authentication/authorization check (Staff only)
                             try {
                                 const payload: SelectTiebreakerSongPayload = await request.json();
                                 if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string') {
                                     return errorResponse("Invalid payload: song_id (number) and selected_difficulty (string) are required.", 400);
                                 }
                                 // Forward POST /api/live-match/:doIdString/select_tiebreaker_song to DO's internal /internal/select-tiebreaker-song
                                 response = await forwardRequestToDO(doIdString, env, request, '/internal/select-tiebreaker-song', 'POST', payload);
                             } catch (e: any) {
                                 console.error(`Worker: Exception processing select_tiebreaker_song payload for DO ${doIdString}:`, e);
                                 response = errorResponse(`Invalid payload format: ${e.message}`, 400);
                             }
                         } else {
                             response = new Response("Method not allowed for /select_tiebreaker_song", { status: 405 });
                         }
                         break;
                    // Add other live match actions here
                    default:
                        response = new Response("Not Found.", { status: 404 });
                        break;
                }
            } else {
                 response = new Response("Invalid live match path format", { status: 400 });
            }
        }

        // --- Match History Endpoint ---
        // GET /api/match_history
        else if (method === 'GET' && pathname === '/api/match_history') {
             try {
                 // Fetch completed/archived matches from tournament_matches
                 const matchesQuery = `
                     SELECT
                         tm.id,
                         tm.round_name,
                         tm.scheduled_time,
                         tm.status,
                         tm.final_score_team1,
                         tm.final_score_team2,
                         t1.name AS team1_name,
                         t2.name AS team2_name,
                         tw.name AS winner_team_name
                     FROM tournament_matches tm
                     JOIN teams t1 ON tm.team1_id = t1.id
                     JOIN teams t2 ON tm.team2_id = t2.id
                     LEFT JOIN teams tw ON tm.winner_team_id = tw.id
                     WHERE tm.status IN ('completed', 'archived')
                     ORDER BY tm.scheduled_time DESC, tm.created_at DESC;
                 `;
                 const { results: matches } = await env.DB.prepare(matchesQuery).all<TournamentMatch>();

                 // For each match, fetch its round history
                 const historyPromises = matches.map(async match => {
                     const roundsQuery = `
                         SELECT
                             mrh.*,
                             s.title AS song_title,
                             s.cover_filename AS cover_filename,
                             t_picker.name AS picker_team_name,
                             m_picker.nickname AS picker_member_nickname,
                             m1.nickname AS team1_member_nickname,
                             m2.nickname AS team2_member_nickname
                         FROM match_rounds_history mrh
                         LEFT JOIN songs s ON mrh.song_id = s.id
                         LEFT JOIN teams t_picker ON mrh.picker_team_id = t_picker.id
                         LEFT JOIN members m_picker ON mrh.picker_member_id = m_picker.id
                         LEFT JOIN members m1 ON mrh.team1_member_id = m1.id
                         LEFT JOIN members m2 ON mrh.team2_member_id = m2.id
                         WHERE mrh.tournament_match_id = ?
                         ORDER BY mrh.round_number_in_match ASC;
                     `;
                     const { results: rounds } = await env.DB.prepare(roundsQuery).bind(match.id).all<any>(); // Use 'any' or a specific history round type

                     // Parse round_summary_json for each round
                     const roundsWithParsedSummary = rounds.map(round => ({
                         ...round,
                         round_summary: round.round_summary_json ? JSON.parse(round.round_summary_json) : null,
                         // Optional: Construct fullCoverUrl for song in history
                         fullCoverUrl: round.cover_filename ? `https://${env.SONG_COVER_BUCKET.name}/${round.cover_filename}` : undefined,
                         // Remove raw JSON field
                         // round_summary_json: undefined,
                     }));

                     return {
                         ...match,
                         rounds: roundsWithParsedSummary,
                     };
                 });

                 const matchHistory = await Promise.all(historyPromises);

                 response = jsonResponse(matchHistory);

             } catch (e: any) {
                 console.error("Worker: Failed to fetch match history:", e);
                 response = errorResponse(e.message);
             }
        }


        // --- Fallback for unmatched routes ---
        else {
            response = new Response('Not Found.', { status: 404 });
        }

        // Add CORS headers to the response
        for (const [key, value] of Object.entries(corsHeaders)) {
            response.headers.set(key, value);
        }
        return response;
    },
};

// Make the Durable Object class available
export { MatchDO };
