import { MatchDO } from './durable-objects/matchDo';
import type { Env } from './types'; // 假设你的 Env 类型定义了 DB

// Helper function to get the singleton MatchDO ID
function getSingletonMatchId(env: Env): DurableObjectId {
  // Using a fixed name for the *current* live match DO instance
  return env.MATCH_DO.idFromName("singleton-match-instance");
}


export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const matchId = getSingletonMatchId(env); // Get the ID for the *current* live match DO
    const matchStub = env.MATCH_DO.get(matchId);

    // Route requests to the Durable Object or handle D1 queries directly

    // WebSocket requests are handled directly by the DO
    if (url.pathname.startsWith('/api/match/websocket')) {
      return matchStub.fetch(request);
    }

    // Forward specific API calls to the DO's internal endpoints
    if (url.pathname === '/api/match/state' && request.method === 'GET') {
        return matchStub.fetch(new Request(url.origin + "/state", request));
    }
    if (url.pathname === '/api/match/update' && request.method === 'POST') {
        return matchStub.fetch(new Request(url.origin + "/update", request));
    }
    // New endpoints forwarded to DO
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


    // New endpoint to get archived round data from D1
    if (url.pathname.startsWith('/api/archived_rounds')) {
        if (request.method === 'GET') {
            try {
                // For now, fetch rounds for the *current* singleton match DO ID
                // If you use unique DO IDs per match, you'd need the match_do_id from the path
                const currentMatchDOId = getSingletonMatchId(env).toString(); // Get the string ID

                // Example: Fetch all rounds for the current match DO, ordered by round number
                const stmt = env.DB.prepare("SELECT * FROM round_archives WHERE match_do_id = ? ORDER BY round_number ASC");
                const { results } = await stmt.bind(currentMatchDOId).all();

                return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
            } catch (e: any) {
                console.error("D1 round query error:", e);
                return new Response(JSON.stringify({ error: "Failed to retrieve archived rounds", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }
        return new Response('Method not allowed for /api/archived_rounds. Use GET.', { status: 405 });
    }

    // Endpoint to get archived match summary data from D1 (modified to use new type)
    if (url.pathname.startsWith('/api/archived_matches')) {
        if (request.method === 'GET') {
            try {
                const pathParts = url.pathname.split('/');
                const archiveId = pathParts[3]; // e.g., /api/archived_matches/{match_do_id}

                if (archiveId) {
                    // Fetch a specific archived match summary
                    const stmt = env.DB.prepare("SELECT * FROM matches_archive WHERE match_do_id = ?");
                    const { results } = await stmt.bind(archiveId).all<MatchArchiveSummary>();
                    if (results && results.length > 0) {
                        return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
                    } else {
                        return new Response(JSON.stringify({ error: "Archived match summary not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                    }
                } else {
                    // Fetch all archived match summaries (consider pagination)
                    const stmt = env.DB.prepare("SELECT id, match_do_id, match_name, final_round, team_a_name, team_b_name, team_a_score, team_b_score, status, archived_at FROM matches_archive ORDER BY archived_at DESC LIMIT 50");
                    const { results } = await stmt.all<MatchArchiveSummary>();
                    return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
                }
            } catch (e: any) {
                console.error("D1 match summary query error:", e);
                return new Response(JSON.stringify({ error: "Failed to retrieve archived match summaries", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }
        return new Response('Method not allowed for /api/archived_matches. Use GET.', { status: 405 });
    }


    // Fallback for other requests or static assets (if any served by this worker)
    return new Response('Not found. API endpoints are /api/match/state, /api/match/update, /api/match/websocket, /api/match/archive-round, /api/match/next-round, /api/match/archive-match, /api/match/new-match, /api/archived_rounds, /api/archived_matches', { status: 404 });
  },
};

// Export the Durable Object class
export { MatchDO };
