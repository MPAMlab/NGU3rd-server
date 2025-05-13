// src/index.ts
import type {
    Env,
    Team,
    Member,
    TournamentMatch,
    MatchState,
    CalculateRoundPayload,
    ResolveDrawPayload,
    MatchScheduleData,
    MemberSongPreference,
    Song,
    MatchSong,
    SelectTiebreakerSongPayload,
    RoundSummary,
    SongLevel,
    ApiResponse,
    SongsApiResponseData,
    SongFiltersApiResponseData,
    PaginationInfo
} from './types'; // Adjust path to your types file
import { MatchDO } from './durable-objects/matchDo'; // Adjust path to your DO file

// Helper to get a DO instance by Name string (using idFromName)
// This fixes the "Invalid Durable Object ID" error
const getMatchDO = (doName: string, env: Env): DurableObjectStub => {
    // Use idFromName to get a valid Durable Object ID from a human-readable name string
    const id: DurableObjectId = env.MATCH_DO.idFromName(doName);
    // Get the Durable Object stub using the derived ID
    return env.MATCH_DO.get(id);
};

// Helper to handle forwarding requests to DOs
const forwardRequestToDO = async (doIdString: string, env: Env, request: Request, internalPath: string, method: string = 'POST', body?: any): Promise<Response> => {
    try {
        const doStub = getMatchDO(doIdString, env);
        // Use a dummy host for the internal URL, as DO fetch ignores the host part
        // and the DO's fetch handler will use the original request's URL if needed (like for websockets)
        const doUrl = new URL(`https://dummy-host`);
        doUrl.pathname = internalPath;

        // 1. Create mutable headers by manually copying from the original request
        const newHeaders = new Headers();
        for (const [key, value] of request.headers.entries()) {
            // Skip the 'Host' header as it's often problematic for internal fetches
            // Also skip headers related to WebSocket upgrade if this isn't a WebSocket request
            // (Though the DO fetch should handle Upgrade headers correctly when present)
            if (key.toLowerCase() !== 'host') {
                 // Note: For WebSocket requests, the 'Upgrade' header is crucial and should be copied.
                 // The manual copy loop handles this correctly as it copies all headers except 'Host'.
                newHeaders.append(key, value);
            }
        }
        // Add any other necessary headers for internal communication if needed
        // e.g., newHeaders.set('X-Worker-Forwarded', 'true');

        // 2. Prepare the body, ensuring it's only added for relevant methods
        let requestBody: BodyInit | null | undefined = undefined;
        if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
            // Use the provided body first, otherwise fallback to the original request's body
            // Note: request.body can only be consumed once. If the original request body
            // is needed by the DO, ensure it hasn't been consumed elsewhere in the Worker.
            // For JSON bodies passed via 'body' parameter, JSON.stringify is used.
            requestBody = body ? JSON.stringify(body) : request.body;
        }

        // 3. Create the Request object configuration
        const requestInit: RequestInit = {
            method: method,
            headers: newHeaders, // Use the newly created mutable headers
            body: requestBody,
            redirect: 'follow', // Standard redirect handling
            // Pass the original cf object for things like IP address, etc.
            cf: request.cf,
        };

        // 4. Create the Request object specifically for the DO fetch
        const doRequest = new Request(doUrl.toString(), requestInit);

        // 5. Explicitly Clone the Request before passing to fetch
        // This creates a completely independent copy, including headers and body (if applicable).
        // This is the most defensive approach against header immutability issues when forwarding.
        // Cloning is particularly important if the original 'request' object or 'doRequest'
        // might be modified or have its body/headers read multiple times before the fetch.
        const clonedDoRequest = doRequest.clone();

        // 6. Fetch using the cloned request
        console.log(`Worker: Forwarding ${method} to DO ${doIdString} at path ${internalPath}`); // Add log
        const response = await doStub.fetch(clonedDoRequest); // *** Use the cloned request ***
        console.log(`Worker: Received response from DO ${doIdString} for path ${internalPath}. Status: ${response.status}`); // Add log

        return response;

    } catch (e: any) {
        console.error(`Worker: Failed to forward request to DO ${doIdString} for path ${internalPath}:`, e);
        // Log the specific error name and message for better debugging
        console.error(`Worker: Error details - Name: ${e.name}, Message: ${e.message}`);
        if (e.stack) {
            console.error(`Worker: Error stack: ${e.stack}`);
        }
        // Return a standard error response format
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

        // CORS headers configuration
        const corsHeadersConfig = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        };

        // Handle CORS preflight requests
        if (method === 'OPTIONS') {
            return new Response(null, {
                headers: corsHeadersConfig,
            });
        }

        let responseFromRoute: Response; // Variable to hold the response generated by the route logic

        // --- API Endpoints (handled by Worker) ---

        // GET /api/teams
        if (method === 'GET' && pathname === '/api/teams') {
            try {
                const { results } = await env.DB.prepare("SELECT * FROM teams").all<Team>();
                responseFromRoute = jsonResponse(results);
            } catch (e: any) {
                console.error("Worker: Failed to list teams:", e);
                responseFromRoute = errorResponse(e.message);
            }
        }
        // GET /api/teams/:id
        else if (method === 'GET' && pathname.match(/^\/api\/teams\/\d+$/)) {
             const parts = pathname.split('/');
             const teamId = parseInt(parts[3], 10);
             if (!isNaN(teamId)) {
                 try {
                     const team = await env.DB.prepare("SELECT * FROM teams WHERE id = ?").bind(teamId).first<Team>();
                     if (team) {
                         responseFromRoute = jsonResponse(team);
                     } else {
                         responseFromRoute = errorResponse("Team not found", 404);
                     }
                 } catch (e: any) {
                     console.error(`Worker: Failed to get team ${teamId}:`, e);
                     responseFromRoute = errorResponse(e.message);
                 }
             } else {
                 responseFromRoute = errorResponse("Invalid team ID in path", 400);
             }
        }
        // GET /api/members
        else if (method === 'GET' && pathname === '/api/members') {
            try {
                const teamCode = url.searchParams.get('team_code');
                let query = "SELECT * FROM members";
                let params: string[] = [];
                if (teamCode) {
                    query += " WHERE team_code = ?";
                    params.push(teamCode);
                }
                const { results } = await env.DB.prepare(query).bind(...params).all<Member>();
                responseFromRoute = jsonResponse(results);
            } catch (e: any) {
                console.error("Worker: Failed to list members:", e);
                responseFromRoute = errorResponse(e.message);
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
                         responseFromRoute = jsonResponse(member);
                     } else {
                         responseFromRoute = errorResponse("Member not found", 404);
                     }
                 } catch (e: any) {
                     console.error(`Worker: Failed to get member ${memberId}:`, e);
                     responseFromRoute = errorResponse(e.message);
                 }
             } else {
                 responseFromRoute = errorResponse("Invalid member ID in path", 400);
             }
        }

        // --- Song Endpoints ---

        // GET /api/songs (Get songs from D1, supports filtering/search/pagination)
        else if (method === 'GET' && pathname === '/api/songs') {
            try {
                // Pagination parameters
                const page = parseInt(url.searchParams.get('page') || '1', 10);
                const limit = parseInt(url.searchParams.get('limit') || '20', 10); // Default limit 20
                const offset = (page - 1) * limit;

                let baseQuery = "FROM songs WHERE 1=1";
                const params: (string | number)[] = [];
                const countParams: (string | number)[] = []; // Separate params for count query

                // Filtering parameters
                const category = url.searchParams.get('category');
                if (category) {
                    baseQuery += " AND category = ?";
                    params.push(category);
                    countParams.push(category);
                }
                const type = url.searchParams.get('type');
                 if (type) {
                     baseQuery += " AND type = ?";
                     params.push(type);
                     countParams.push(type);
                 }
                 const search = url.searchParams.get('search');
                 if (search) {
                     baseQuery += " AND title LIKE ?";
                     params.push(`%${search}%`);
                     countParams.push(`%${search}%`);
                 }
                 // TODO: Add backend filtering by level if needed (requires JSON functions or schema change)
                 // For now, level filtering is client-side.

                 // Construct count and data queries
                 const countQuery = `SELECT COUNT(*) as total ${baseQuery}`;
                 const dataQuery = `SELECT * ${baseQuery} ORDER BY title ASC LIMIT ? OFFSET ?`; // Add ORDER BY, LIMIT, OFFSET
                 params.push(limit, offset); // Add limit and offset to data query params

                // Execute both queries concurrently
                const countStmt = env.DB.prepare(countQuery).bind(...countParams);
                const dataStmt = env.DB.prepare(dataQuery).bind(...params);

                const [{ total }, { results }] = await Promise.all([
                    countStmt.first<{ total: number }>(), // Get total count
                    dataStmt.all<Song>() // Get data for the current page
                ]);

                // Parse levels_json and construct fullCoverUrl for each song on the current page
                const songsWithDetails = results.map((song) => {
                    if (!song) { // Explicit check for undefined/null
                         console.error(`Worker: /api/songs - Encountered null/undefined song in results.`);
                         return null; // Return null for undefined/null entries
                    }

                    const parsedLevels = song.levels_json ? JSON.parse(song.levels_json) as SongLevel : undefined;
                    // Use the R2_PUBLIC_BUCKET_URL environment variable
                    const fullCoverUrl = song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                        ? `${env.R2_PUBLIC_BUCKET_URL}/${song.cover_filename}`
                        : undefined;

                    return { ...song, parsedLevels, fullCoverUrl };
                }).filter(song => song !== null); // Filter out any null entries

                // Prepare pagination info
                const pagination: PaginationInfo = {
                    currentPage: page,
                    pageSize: limit,
                    totalItems: total ?? 0, // Use the count result, default to 0 if null
                    totalPages: Math.ceil((total ?? 0) / limit)
                };

                // Return structured response with songs and pagination
                responseFromRoute = jsonResponse<SongsApiResponseData>({
                    songs: songsWithDetails,
                    pagination: pagination
                });

            } catch (e: any) {
                console.error("Worker: Failed to list songs:", e);
                // --- DEBUG LOG: Log D1 error cause if available ---
                if (e.cause) {
                    console.error("Worker: D1 Error Cause:", e.cause);
                }
                // --- END DEBUG LOG ---
                responseFromRoute = errorResponse(e.message);
            }
        }

        // GET /api/songs/filters (New endpoint to get distinct categories and types)
        else if (method === 'GET' && pathname === '/api/songs/filters') {
            try {
                const categoryQuery = "SELECT DISTINCT category FROM songs WHERE category IS NOT NULL AND category != '' ORDER BY category";
                const typeQuery = "SELECT DISTINCT type FROM songs WHERE type IS NOT NULL AND type != '' ORDER BY type";

                const categoryStmt = env.DB.prepare(categoryQuery);
                const typeStmt = env.DB.prepare(typeQuery);

                // Execute both queries concurrently
                const [{ results: categories }, { results: types }] = await Promise.all([
                    categoryStmt.all<{ category: string }>(),
                    typeStmt.all<{ type: string }>()
                ]);

                // Extract values and filter out potential null/empty strings
                const categoryList = categories.map(c => c.category).filter(Boolean);
                const typeList = types.map(t => t.type).filter(Boolean);

                responseFromRoute = jsonResponse<SongFiltersApiResponseData>({
                    categories: categoryList,
                    types: typeList
                });

            } catch (e: any) {
                console.error("Worker: Failed to get song filter options:", e);
                responseFromRoute = errorResponse(e.message);
            }
        }


        // --- New Member Song Preference Endpoints ---

        // POST /api/member_song_preferences
        else if (method === 'POST' && pathname === '/api/member_song_preferences') {
             // TODO: Add authentication/authorization check (ensure member_id matches logged-in user or admin)
             try {
                 const payload: MemberSongPreference = await request.json();

                 if (!payload.member_id || !payload.tournament_stage || !payload.song_id || !payload.selected_difficulty) {
                     responseFromRoute = errorResponse("Missing required fields for member song preference.", 400);
                 } else {
                     // Optional: Validate member_id, song_id exist in DB
                     const memberExists = await env.DB.prepare("SELECT id FROM members WHERE id = ?").bind(payload.member_id).first();
                     const songExists = await env.DB.prepare("SELECT id FROM songs WHERE id = ?").bind(payload.song_id).first();
                     if (!memberExists || !songExists) {
                          responseFromRoute = errorResponse("Invalid member_id or song_id.", 400);
                     } else {

                         const stmt = env.DB.prepare(
                             `INSERT INTO member_song_preferences (member_id, tournament_stage, song_id, selected_difficulty, created_at)
                              VALUES (?, ?, ?, ?, ?)
                              ON CONFLICT(member_id, tournament_stage, song_id, selected_difficulty) DO UPDATE SET
                                  selected_difficulty = excluded.selected_difficulty, -- Allow updating difficulty
                                  created_at = excluded.created_at -- Update timestamp
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
                             // Fetch the inserted/updated preference to return, including song details
                             // Use last_row_id for INSERT, need to re-query for UPDATE case or fetch by unique constraint
                             // For simplicity, let's re-fetch using the unique constraint for both cases
                             const newPreferenceQuery = `
                                 SELECT
                                     msp.*,
                                     s.title AS song_title,
                                     s.cover_filename AS cover_filename,
                                     s.levels_json AS levels_json
                                 FROM member_song_preferences msp
                                 JOIN songs s ON msp.song_id = s.id
                                 WHERE msp.member_id = ? AND msp.tournament_stage = ? AND msp.song_id = ? AND msp.selected_difficulty = ?`; // Use unique constraint
                             const newPreference = await env.DB.prepare(newPreferenceQuery).bind(
                                 payload.member_id,
                                 payload.tournament_stage,
                                 payload.song_id,
                                 payload.selected_difficulty
                             ).first<MemberSongPreference & { levels_json: string | null; song_title: string; cover_filename: string | null }>();


                             // Add parsed levels and full cover URL
                             if (newPreference) {
                                  (newPreference as MemberSongPreference).parsedLevels = newPreference.levels_json ? JSON.parse(newPreference.levels_json) : undefined;
                                  // Use the R2_PUBLIC_BUCKET_URL environment variable
                                  (newPreference as MemberSongPreference).fullCoverUrl = newPreference.cover_filename && env.R2_PUBLIC_BUCKET_URL
                                    ? `${env.R2_PUBLIC_BUCKET_URL}/${newPreference.cover_filename}`
                                    : undefined;
                                  delete (newPreference as any).levels_json; // Remove raw json field
                             }


                             responseFromRoute = jsonResponse(newPreference, 201);
                         } else {
                             console.error("Worker: Failed to save member song preference:", result.error);
                             responseFromRoute = errorResponse(result.error || "Failed to save preference.");
                         }
                     }
                 }

             } catch (e: any) {
                 console.error("Worker: Exception saving member song preference:", e);
                 responseFromRoute = errorResponse(e.message);
             }
        }

        // GET /api/member_song_preferences?member_id=:id&stage=:stage
        else if (method === 'GET' && pathname === '/api/member_song_preferences') {
             // TODO: Add authentication/authorization check (ensure member_id matches logged-in user or admin)
             try {
                 const memberId = url.searchParams.get('member_id');
                 const stage = url.searchParams.get('stage');

                 if (!memberId || !stage) {
                     responseFromRoute = errorResponse("Missing member_id or stage query parameter.", 400);
                 } else {

                     const memberIdNum = parseInt(memberId, 10);
                     if (isNaN(memberIdNum)) {
                         responseFromRoute = errorResponse("Invalid member_id.", 400);
                     } else {

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

                         const { results } = await env.DB.prepare(query).bind(memberIdNum, stage).all<MemberSongPreference & { levels_json: string | null; song_title: string; cover_filename: string | null }>();

                         // Parse levels_json and construct fullCoverUrl for each preference
                         const preferencesWithDetails = results.map(pref => {
                             const parsedLevels = pref.levels_json ? JSON.parse(pref.levels_json) as SongLevel : undefined;
                             // Use the R2_PUBLIC_BUCKET_URL environment variable
                             const fullCoverUrl = pref.cover_filename && env.R2_PUBLIC_BUCKET_URL
                                 ? `${env.R2_PUBLIC_BUCKET_URL}/${pref.cover_filename}`
                                 : undefined;
                             // Remove levels_json from the final object if you don't want to expose it directly
                             const { levels_json, ...rest } = pref;
                             return { ...rest, parsedLevels, fullCoverUrl };
                         });


                         responseFromRoute = jsonResponse(preferencesWithDetails);
                     }
                 }

             } catch (e: any) {
                 console.error("Worker: Failed to get member song preferences:", e);
                 responseFromRoute = errorResponse(e.message);
             }
        }


        // --- Tournament Match Endpoints (Updated) ---

        // GET /api/tournament_matches (Updated to parse JSON fields and fixed ORDER BY semicolon)
        else if (method === 'GET' && pathname === '/api/tournament_matches') {
            try {
                // --- Original query without the trailing semicolon ---
                let query = `
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
                    ORDER BY tm.created_at DESC
                `; // Removed the trailing semicolon ;

                console.log("Worker: /api/tournament_matches - Testing with SQL Query String:", query);

                const { results } = await env.DB.prepare(query).all<TournamentMatch>();

                // Restore original processing to parse JSON fields
                const matchesWithParsedData = results.map(match => ({
                    ...match,
                    team1_player_order: match.team1_player_order_json ? JSON.parse(match.team1_player_order_json) as number[] : null,
                    team2_player_order: match.team2_player_order_json ? JSON.parse(match.team2_player_order_json) as number[] : null,
                    match_song_list: match.match_song_list_json ? JSON.parse(match.match_song_list_json) as MatchSong[] : null,
                }));

                responseFromRoute = jsonResponse(matchesWithParsedData);

            } catch (e: any) {
                console.error("Worker: Failed to list tournament matches:", e);
                if (e.cause) {
                    console.error("Worker: D1 Error Cause:", e.cause);
                }
                responseFromRoute = errorResponse(e.message);
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
                     responseFromRoute = errorResponse("Missing required fields: round_name, team1_id, team2_id", 400);
                } else {

                    // Basic validation for team IDs
                    const team1 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team1_id).first();
                    const team2 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team2_id).first();
                    if (!team1 || !team2) {
                         responseFromRoute = errorResponse("Invalid team1_id or team2_id", 400);
                    } else {

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
                            responseFromRoute = jsonResponse(newMatch, 201);
                        } else {
                            console.error("Worker: Failed to create tournament match:", result.error);
                            responseFromRoute = errorResponse(result.error || "Failed to create match.");
                        }
                    }
                }

            } catch (e: any) {
                console.error("Worker: Exception creating tournament match:", e);
                responseFromRoute = errorResponse(e.message);
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
                         responseFromRoute = errorResponse("Invalid payload: player orders and song list must be non-empty arrays.", 400);
                     } else {
                         // TODO: More robust validation: check if player IDs exist and belong to the correct teams, check if song IDs exist.

                         // Fetch the match to ensure it exists and is in a valid state (e.g., 'scheduled' or 'pending_song_confirmation')
                         const match = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<TournamentMatch>();
                         if (!match) {
                             responseFromRoute = errorResponse("Tournament match not found.", 404);
                         } else if (match.status !== 'scheduled' && match.status !== 'pending_song_confirmation') {
                              responseFromRoute = errorResponse(`Match status is '${match.status}'. Must be 'scheduled' or 'pending_song_confirmation' to confirm setup.`, 400);
                         } else {


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
                                 responseFromRoute = jsonResponse(updatedMatch);
                             } else {
                                 console.error("Worker: Failed to confirm match setup:", result.error);
                                 responseFromRoute = errorResponse(result.error || "Failed to confirm setup.");
                             }
                         }
                     }

                 } catch (e: any) {
                     console.error(`Worker: Exception confirming match setup ${tournamentMatchId}:`, e);
                     responseFromRoute = errorResponse(e.message);
                 }
             } else {
                 responseFromRoute = errorResponse("Invalid tournament match ID", 400);
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
                         responseFromRoute = errorResponse("Scheduled match not found", 404);
                     } else if (match.status === 'live' && match.match_do_id) {
                         // If already live, just return the existing DO ID
                         console.log(`Worker: Match ${tournamentMatchId} is already live with DO ${match.match_do_id}. Returning existing DO ID.`);
                         responseFromRoute = jsonResponse({ message: "Match is already live.", match_do_id: match.match_do_id });
                     } else if (match.status !== 'ready_to_start') {
                          responseFromRoute = errorResponse(`Match status is '${match.status}'. Must be 'ready_to_start' to start live.`, 400);
                     } else if (!match.team1_player_order_json || !match.team2_player_order_json || !match.match_song_list_json) {
                          responseFromRoute = errorResponse("Match setup is incomplete (player order or song list missing).", 400);
                     } else {

                         // 2. Fetch member details for both teams
                         // Assuming members table uses team_code, need to get team codes first
                         const team1 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team1_id).first<{ code: string }>();
                         const team2 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team2_id).first<{ code: string }>();
                         if (!team1 || !team2) {
                              responseFromRoute = errorResponse("Could not fetch team codes for members.", 500);
                         } else {

                             const team1Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team1.code).all<Member>();
                             const team2Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team2.code).all<Member>();

                             if (!team1Members.results || team1Members.results.length === 0 || !team2Members.results || team2Members.results.length === 0) {
                                   // This might happen if teams have no members assigned yet, which is a valid state for setup,
                                   // but not for starting a live match. Add a check here.
                                   responseFromRoute = errorResponse("One or both teams have no members assigned.", 400);
                              } else {

                                 // 3. Parse player order and song list JSON
                                 const team1PlayerOrderIds: number[] = JSON.parse(match.team1_player_order_json);
                                 const team2PlayerOrderIds: number[] = JSON.parse(match.team2_player_order_json);
                                 const matchSongList: MatchSong[] = JSON.parse(match.match_song_list_json);

                                 // Basic validation for parsed data
                                 if (!Array.isArray(team1PlayerOrderIds) || !Array.isArray(team2PlayerOrderIds) || !Array.isArray(matchSongList) || team1PlayerOrderIds.length === 0 || team2PlayerOrderIds.length === 0 || matchSongList.length === 0) {
                                      responseFromRoute = errorResponse("Parsed setup data is invalid.", 500); // Should not happen if confirm_setup validated
                                 } else {

                                     // 4. Generate a DO ID (e.g., based on tournamentMatchId)
                                     // Use a consistent name based on the match ID
                                     const doIdString = `match-${tournamentMatchId}`;

                                     // 5. Get the DO instance and prepare initialization payload
                                     // getMatchDO now uses idFromName, resolving the error
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
                                              responseFromRoute = jsonResponse({ message: "Match started live.", match_do_id: doIdString, do_init_result: doResult });
                                         } else {
                                              console.error(`Worker: Failed to update tournament_matches status for ${tournamentMatchId}:`, updateResult.error);
                                              responseFromRoute = errorResponse(`Match started live in DO, but failed to update schedule status: ${updateResult.error}`, 500);
                                         }
                                     } else {
                                         // DO initialization failed
                                         const errorBody = await doResponse.json(); // Assuming DO returns JSON error
                                         console.error(`Worker: Failed to initialize DO ${doIdString} for match ${tournamentMatchId}:`, errorBody);
                                         responseFromRoute = errorResponse(`Failed to initialize live match in Durable Object: ${errorBody.message || errorBody.error}`, doResponse.status);
                                     }
                                 }
                              }
                         }
                     }

                 } catch (e: any) {
                     console.error(`Worker: Exception starting live match ${tournamentMatchId}:`, e);
                     responseFromRoute = errorResponse(`Exception starting live match: ${e.message}`);
                 }
             } else {
                 responseFromRoute = errorResponse("Invalid tournament match ID", 400);
             }
        }

        // --- API Endpoints for Live Matches (forwarded to DO) ---
        // These paths have the structure /api/live-match/:doIdString/...
        else if (pathname.startsWith('/api/live-match/')) {
            const parts = pathname.split('/');
            const doIdString = parts[3]; // e.g., /api/live-match/some-id/state -> parts = ["", "api", "live-match", "some-id", "state"]
            const action = parts[4]; // e.g., "state", "websocket", "calculate-round", etc.

            if (parts.length >= 4 && doIdString) {
                switch (action) {
                    case 'state':
                        if (method === 'GET' && parts.length === 5) {
                            // getMatchDO now correctly uses idFromName
                            responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/state', 'GET');
                        } else {
                            responseFromRoute = new Response("Method not allowed for /state", { status: 405 });
                        }
                        break;
                    case 'websocket': // WebSocket forwarding needs special handling
                        if (method === 'GET' && parts.length === 5) {
                             const doStub = getMatchDO(doIdString, env);
                             const doUrl = new URL(request.url); // Use original request URL for WebSocket
                             doUrl.pathname = '/websocket'; // Target DO's /websocket path
                             // For WebSockets, you forward the original request more directly
                             // The DO's fetch handler will handle the Upgrade header.
                             // This response is a special 101 Switching Protocols response.
                             responseFromRoute = await doStub.fetch(doUrl.toString(), request);
                             // !! IMPORTANT: Do NOT modify this response. It must be returned directly. !!
                        } else {
                            responseFromRoute = new Response("Method not allowed for /websocket", { status: 405 });
                        }
                        break;
                    case 'calculate-round': // Corrected: hyphen
                        if (method === 'POST' && parts.length === 5) {
                            try {
                                const payload: CalculateRoundPayload = await request.json();
                                if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
                                     responseFromRoute = errorResponse("Invalid payload: teamA_percentage and teamB_percentage must be numbers.", 400);
                                } else {
                                    // getMatchDO now correctly uses idFromName
                                    responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/internal/calculate-round', 'POST', payload);
                                }
                            } catch (e: any) {
                                console.error(`Worker: Exception processing calculate-round payload for DO ${doIdString}:`, e);
                                responseFromRoute = errorResponse(`Invalid payload format: ${e.message}`, 400);
                            }
                        } else {
                            responseFromRoute = new Response("Method not allowed for /calculate-round", { status: 405 });
                        }
                        break;
                    case 'next-round': // Corrected: hyphen
                        if (method === 'POST' && parts.length === 5) {
                            // getMatchDO now correctly uses idFromName
                            responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/internal/next-round', 'POST');
                        } else {
                            responseFromRoute = new Response("Method not allowed for /next-round", { status: 405 });
                        }
                        break;
                    case 'archive': // Already hyphen
                         if (method === 'POST' && parts.length === 5) {
                             // getMatchDO now correctly uses idFromName
                             responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/internal/archive-match', 'POST');
                         } else {
                             responseFromRoute = new Response("Method not allowed for /archive", { status: 405 });
                         }
                         break;
                    case 'resolve-draw': // Corrected: hyphen
                         if (method === 'POST' && parts.length === 5) {
                             try {
                                 const payload: ResolveDrawPayload = await request.json();
                                 if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
                                      responseFromRoute = errorResponse("Invalid payload: winner must be 'teamA' or 'teamB'.", 400);
                                  } else {
                                      // getMatchDO now correctly uses idFromName
                                      responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/internal/resolve-draw', 'POST', payload);
                                  }
                             } catch (e: any) {
                                 console.error(`Worker: Exception processing resolve-draw payload for DO ${doIdString}:`, e);
                                 responseFromRoute = errorResponse(`Invalid payload format: ${e.message}`, 400);
                             }
                         } else {
                             responseFromRoute = new Response("Method not allowed for /resolve-draw", { status: 405 });
                         }
                         break;
                    case 'select-tiebreaker-song': // Corrected: hyphen
                         if (method === 'POST' && parts.length === 5) {
                             // TODO: Add authentication/authorization check (Staff only)
                             try {
                                 const payload: SelectTiebreakerSongPayload = await request.json();
                                 if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string') {
                                     responseFromRoute = errorResponse("Invalid select-tiebreaker-song payload: song_id (number) and selected_difficulty (string) are required.", 400);
                                 } else {
                                     // Before forwarding, fetch song details from D1 to pass to DO
                                     const song = await env.DB.prepare("SELECT * FROM songs WHERE id = ?").bind(payload.song_id).first<Song>();
                                     if (!song) {
                                          responseFromRoute = errorResponse(`Song with ID ${payload.song_id} not found in D1.`, 404);
                                      } else {
                                          // Pass song details along with selection to DO
                                         const doPayload = { song_id: payload.song_id, selected_difficulty: payload.selected_difficulty, song_details: song };
                                          // getMatchDO now correctly uses idFromName
                                         responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/internal/select-tiebreaker-song', 'POST', doPayload);
                                      }
                                 }
                             } catch (e: any) {
                                 console.error(`Worker: Exception processing select-tiebreaker-song payload for DO ${doIdString}:`, e);
                                 responseFromRoute = errorResponse(`Invalid payload format: ${e.message}`, 400);
                             }
                         } else {
                             responseFromRoute = new Response("Method not allowed for /select-tiebreaker-song", { status: 405 });
                         }
                         break;
                    default:
                        responseFromRoute = new Response("Not Found.", { status: 404 });
                        break;
                }
            } else {
                 responseFromRoute = new Response("Invalid live match path format", { status: 400 });
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
                 const { results } = await env.DB.prepare(matchesQuery).all<TournamentMatch>();

                 // For each match, fetch its round history
                 const historyPromises = results.map(async match => {
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

                     // Parse round_summary_json and add fullCoverUrl for each round
                     const roundsWithParsedData = rounds.map(round => ({
                         ...round,
                         round_summary: round.round_summary_json ? JSON.parse(round.round_summary_json) as RoundSummary : null,
                         // Use the R2_PUBLIC_BUCKET_URL environment variable
                         fullCoverUrl: round.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/${round.cover_filename}`
                            : undefined,
                         // Remove raw JSON field
                         // round_summary_json: undefined,
                     }));

                     return {
                         ...match,
                         rounds: roundsWithParsedData,
                     };
                 });

                 const matchHistory = await Promise.all(historyPromises);

                 responseFromRoute = jsonResponse(matchHistory);

             } catch (e: any) {
                 console.error("Worker: Failed to fetch match history:", e);
                 responseFromRoute = errorResponse(e.message);
             }
        }


        // --- Fallback for unmatched routes ---
        else {
            responseFromRoute = new Response('Not Found.', { status: 404 });
        }

        // --- Add CORS headers *UNLESS* it's a WebSocket upgrade response ---

        // Check if the response from the route logic indicates a successful WebSocket upgrade.
        // The `webSocket` property will be non-null in this case.
        if (responseFromRoute.webSocket) {
            // If it's a WebSocket upgrade response, return it directly without modification.
            // Adding CORS headers or creating a new Response object breaks the handshake.
            console.log("Worker: Returning WebSocket upgrade response directly.");
            return responseFromRoute;
        }

        // If it's NOT a WebSocket upgrade, create a new mutable Response to add CORS headers.
        console.log(`Worker: Adding CORS headers to standard response (Status: ${responseFromRoute.status})`);
        const finalResponse = new Response(responseFromRoute.body, responseFromRoute);

        // Add/overwrite CORS headers on the mutable 'finalResponse.headers'
        for (const [key, value] of Object.entries(corsHeadersConfig)) {
            finalResponse.headers.set(key, value);
        }

        return finalResponse;
    },
};

// Make the Durable Object class available
export { MatchDO };
