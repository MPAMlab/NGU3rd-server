// src/handlers/live/state.ts

import { Request, Env, LiveMatchState } from '../../types';
import { apiResponse, apiError } from '../../index'; // Assuming these are exported from index.ts

// GET /api/live/state/:matchId (Public - No auth needed)
export async function handleGetLiveState(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split('/');
    // Expecting path like /api/live/state/123
    if (parts.length !== 5 || !parts[4]) {
        return apiError('Invalid API path. Use /api/live/state/:matchId', 400);
    }
    const matchId = parseInt(parts[4], 10);

    if (isNaN(matchId)) {
        return apiError('Invalid match ID format.', 400);
    }

    console.log(`Handling /api/live/state/${matchId} request...`);

    try {
        // Get the Durable Object ID for this match
        const doId = env.LIVE_MATCH_DO.idFromName(matchId.toString());
        // Get the Durable Object stub
        const stub = env.LIVE_MATCH_DO.get(doId);

        // Send an internal request to the DO to get its current state
        const doResponse = await stub.fetch(new Request(stub.id.toString() + '/get-state', {
             method: 'GET',
             // No body needed for GET
        }));

        if (!doResponse.ok) {
            // DO might return 404 if state isn't initialized or match doesn't exist
            console.warn(`DO ${matchId} returned ${doResponse.status} for get-state.`);
            return apiError('Live state not available for this match.', doResponse.status);
        }

        const liveState: LiveMatchState = await doResponse.json();
        return apiResponse(liveState, 200);

    } catch (e) {
        console.error(`Error fetching live state for match ${matchId} from DO:`, e);
        return apiError('Failed to fetch live match state.', 500, e);
    }
}
