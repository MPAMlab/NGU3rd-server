import { MatchDO } from './durable-objects/matchDo';
import type { Env, RoundArchive, MatchArchiveSummary } from './types'; // Import types

// Helper function to get the singleton MatchDO ID
function getSingletonMatchId(env: Env): DurableObjectId {
  // Using a fixed name for the *current* live match DO instance
  return env.MATCH_DO.idFromName("singleton-match-instance");
}

// Helper to determine winner based on scores (duplicated from DO for D1 updates)
function determineWinner(state: { team_a_score: number; team_b_score: number; team_a_name: string; team_b_name: string }): string | null {
    if (state.team_a_score > state.team_b_score) {
        return state.team_a_name || '队伍A';
    } else if (state.team_b_score > state.team_a_score) {
        return state.team_b_name || '队伍B';
    } else {
        return null; // Draw or undecided
    }
}


export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const matchId = getSingletonMatchId(env); // Get the ID for the *current* live match DO
    const matchStub = env.MATCH_DO.get(matchId);

    // --- Route requests ---

    // WebSocket requests are forwarded to the DO
    if (url.pathname === '/api/match/websocket') {
      // Forward the request to the DO's internal websocket path
      return matchStub.fetch(new Request(url.origin + "/websocket", request));
    }

    // Forward specific API calls related to the *current* match state to the DO's internal endpoints
    if (url.pathname === '/api/match/state' && request.method === 'GET') {
        return matchStub.fetch(new Request(url.origin + "/state", request));
    }
    if (url.pathname === '/api/match/update' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/update", request));
    }
    if (url.pathname === '/api/match/archive-round' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/internal/archive-round", request));
    }
    if (url.pathname === '/api/match/next-round' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/internal/next-round", request));
    }
    if (url.pathname === '/api/match/archive-match' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/internal/archive-match", request));
    }
     if (url.pathname === '/api/match/new-match' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/internal/new-match", request));
    }


    // --- Handle D1 Queries/Updates directly in the Worker ---

    // Endpoint to get archived round data from D1 for the *current* singleton match DO ID
    if (url.pathname === '/api/archived_rounds' && request.method === 'GET') {
        try {
            const currentMatchDOId = getSingletonMatchId(env).toString(); // Get the string ID

            // Fetch all rounds for the current match DO, ordered by round number
            const stmt = env.DB.prepare("SELECT * FROM round_archives WHERE match_do_id = ? ORDER BY round_number ASC");
            const { results } = await stmt.bind(currentMatchDOId).all<RoundArchive>();

            return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
        } catch (e: any) {
            console.error("Worker D1 round query error:", e);
            return new Response(JSON.stringify({ error: "Failed to retrieve archived rounds", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }

    // Endpoint to update an archived round in D1
    // This request does NOT go to the DO, it interacts with D1 directly from the worker
    if (url.pathname.startsWith('/api/archived_rounds/') && request.method === 'PUT') {
        const pathParts = url.pathname.split('/');
        const roundArchiveId = parseInt(pathParts[pathParts.length - 1], 10); // Get ID from path

        if (isNaN(roundArchiveId)) {
            return new Response(JSON.stringify({ error: "Invalid round archive ID in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }

        try {
            const updates = await request.json<Partial<RoundArchive>>();

            // Fetch the existing record to get current names/scores if not provided in updates
            const existingStmt = env.DB.prepare("SELECT team_a_name, team_a_score, team_b_name, team_b_score FROM round_archives WHERE id = ?").bind(roundArchiveId);
            const { results: existingResults } = await existingStmt.all<{ team_a_name: string, team_a_score: number, team_b_name: string, team_b_score: number }>();

            if (!existingResults || existingResults.length === 0) {
                 return new Response(JSON.stringify({ error: "Archived round not found for update" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
            }
            const existingRecord = existingResults[0];

            // Merge updates with existing data to determine final scores/names for winner calculation
            const mergedDataForWinner = {
                team_a_name: updates.team_a_name ?? existingRecord.team_a_name,
                team_a_score: updates.team_a_score ?? existingRecord.team_a_score,
                team_b_name: updates.team_b_name ?? existingRecord.team_b_name,
                team_b_score: updates.team_b_score ?? existingRecord.team_b_score,
            };

            // Recalculate winner based on the merged data
            const winnerName = determineWinner(mergedDataForWinner);


            const stmt = env.DB.prepare(
                `UPDATE round_archives SET
                   team_a_name = COALESCE(?, team_a_name),
                   team_a_score = COALESCE(?, team_a_score),
                   team_a_player = COALESCE(?, team_a_player),
                   team_b_name = COALESCE(?, team_b_name),
                   team_b_score = COALESCE(?, team_b_score),
                   team_b_player = COALESCE(?, team_b_player),
                   status = COALESCE(?, status),
                   winner_team_name = ? -- Always bind winnerName (can be null)
                 WHERE id = ?`
            );

            const result = await stmt.bind(
                updates.team_a_name, updates.team_a_score, updates.team_a_player,
                updates.team_b_name, updates.team_b_score, updates.team_b_player,
                updates.status,
                winnerName, // Bind the calculated winner name
                roundArchiveId
            ).run();

            if (result.success) {
                console.log(`Worker: Archived round ${roundArchiveId} updated in D1.`);
                // Fetch the updated record to return it to the frontend
                const fetchUpdatedStmt = env.DB.prepare("SELECT * FROM round_archives WHERE id = ?").bind(roundArchiveId);
                const { results: updatedResults } = await fetchUpdatedStmt.all<RoundArchive>();

                return new Response(JSON.stringify({ success: true, message: "Archived round updated.", updatedRecord: updatedResults ? updatedResults[0] : null }), { headers: { 'Content-Type': 'application/json' } });
            } else {
                console.error(`Worker: Failed to update archived round ${roundArchiveId} in D1:`, result.error);
                return new Response(JSON.stringify({ success: false, message: `Failed to update archived round: ${result.error}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }

        } catch (e: any) {
            console.error(`Worker: Exception during D1 archived round update:`, e);
            return new Response(JSON.stringify({ error: `Exception during archived round update: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // Endpoint to get archived match summary data from D1
    if (url.pathname.startsWith('/api/archived_matches') && request.method === 'GET') {
        try {
            const pathParts = url.pathname.split('/');
            const archiveId = pathParts[3]; // e.g., /api/archived_matches/{match_do_id}

            if (archiveId) {
                // Fetch a specific archived match summary by DO ID
                const stmt = env.DB.prepare("SELECT * FROM matches_archive WHERE match_do_id = ?");
                const { results } = await stmt.bind(archiveId).all<MatchArchiveSummary>();
                if (results && results.length > 0) {
                    return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
                } else {
                    return new Response(JSON.stringify({ error: "Archived match summary not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                }
            } else {
                // Fetch all archived match summaries (consider pagination for large lists)
                const stmt = env.DB.prepare("SELECT id, match_do_id, match_name, final_round, team_a_name, team_b_name, team_a_score, team_b_score, winner_team_name, status, archived_at FROM matches_archive ORDER BY archived_at DESC LIMIT 50");
                const { results } = await stmt.all<MatchArchiveSummary>();
                return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
            }
        } catch (e: any) {
            console.error("Worker D1 match summary query error:", e);
            return new Response(JSON.stringify({ error: "Failed to retrieve archived match summaries", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // Fallback for other requests or static assets (if any served by this worker)
    return new Response('Not found. API endpoints are /api/match/*, /api/archived_rounds, /api/archived_rounds/:id (PUT), /api/archived_matches', { status: 404 });
  },
};

// Export the Durable Object class
export { MatchDO };
