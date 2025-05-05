// src/index.ts

// --- Imports ---
import { Env, AuthenticatedRequest } from './types';
import { ExecutionContext } from "@cloudflare/workers-types"; // Standard Worker types

// Import Handlers
import { handleKindeLogin, handleKindeCallback, handleKindeLogout } from './handlers/kinde/auth';
import { handleFetchMe } from './handlers/user/members';
import { handleGetTeamByCode } from './handlers/public/teams';
import { handleGetSettings } from './handlers/public/settings';
import { handleGetLiveState } from './handlers/live/state';
import {
    handleAdminFetchMatches,
    handleAdminGetMatch,
    handleAdminCreateMatch,
    handleAdminStartMatch,
    handleAdminRecordTurn,
    handleAdminEndMatch,
    handleAdminGetRandomSong,
    handleAdminSetFinalSongs,
} from './handlers/admin/matches';
// Import other handlers as you create them

// Import Middlewares
import { authMiddleware, adminAuthMiddleware } from './utils/auth';

// Import Durable Object Class
import { LiveMatchDO } from './durableobjects/LiveMatchDO';


// --- Configuration & Constants ---
export const CORS_HEADERS = { // Exported for use in handlers
    'Access-Control-Allow-Origin': '*', // Or restrict to your frontend domain
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Cookie', // Include Authorization header
    'Access-Control-Max-Age': '86400', // Cache preflight requests for 24 hours
    'Access-Control-Allow-Credentials': 'true', // Important for cookies
};

// Basic API Response/Error helpers (Exported for use in handlers)
export function apiResponse(data: any, status: number = 200): Response {
    return new Response(JSON.stringify(data), {
        status: status,
        headers: {
            'Content-Type': 'application/json',
            ...CORS_HEADERS,
        },
    });
}

export function apiError(message: string, status: number = 500, error?: any): Response {
    console.error(`API Error (${status}): ${message}`, error);
    // In production, you might not want to send the raw error object to the client
    const errorBody = { error: message };
    if (error && typeof error === 'string') { // Include simple string errors
         // errorBody.details = error; // Optional: include error details
    } else if (error instanceof Error) {
         // errorBody.details = error.message; // Optional: include error message
    }

    return new Response(JSON.stringify(errorBody), {
        status: status,
        headers: {
            'Content-Type': 'application/json',
            ...CORS_HEADERS,
        },
    });
}


// Helper to add CORS headers to a response
function addCorsHeaders(response: Response, origin: string | null): Response {
    // In this setup, CORS_HEADERS are already added by apiResponse/apiError.
    // This function is primarily needed for the OPTIONS preflight response.
    // For other responses, the headers are already set.
     if (origin && CORS_HEADERS['Access-Control-Allow-Origin'] === '*') {
         response.headers.set('Access-Control-Allow-Origin', origin); // Reflect the origin if wildcard is used
     } else if (origin && CORS_HEADERS['Access-Control-Allow-Origin'] !== '*' && CORS_HEADERS['Access-Control-Allow-Origin'].includes(origin)) {
         response.headers.set('Access-Control-Allow-Origin', origin); // Explicitly set if origin is in allowed list
     }
     // Other CORS headers are already in CORS_HEADERS constant
    return response;
}


// Worker entry point
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const method = request.method;
    const origin = request.headers.get('Origin'); // Get the origin for CORS

    // Cast request to AuthenticatedRequest to allow adding properties
    const authenticatedRequest = request as AuthenticatedRequest;

    // --- Handle CORS Preflight (OPTIONS method) ---
    if (method === 'OPTIONS') {
        let response = new Response(null, { status: 204 }); // No content needed for preflight success
        response.headers.set('Access-Control-Max-Age', '86400'); // Cache preflight for 24 hours
        // Manually add other CORS headers for preflight
        response.headers.set('Access-Control-Allow-Methods', CORS_HEADERS['Access-Control-Allow-Methods']);
        response.headers.set('Access-Control-Allow-Headers', CORS_HEADERS['Access-Control-Allow-Headers']);
        response.headers.set('Access-Control-Allow-Credentials', CORS_HEADERS['Access-Control-Allow-Credentials']);
        if (origin) {
             response.headers.set('Access-Control-Allow-Origin', origin);
        } else {
             response.headers.set('Access-Control-Allow-Origin', CORS_HEADERS['Access-Control-Allow-Origin']);
        }
        return response;
    }

    // --- Route Handling using if-else ---

    let response: Response | void; // Variable to hold potential response from middleware or handler

    // Kinde Authentication Routes (App 2) - Public
    if (pathname === '/login' && method === 'GET') {
        response = handleKindeLogin(authenticatedRequest, env);
    } else if (pathname === '/api/kinde/callback' && method === 'POST') { // Matches App 1 frontend POST
        response = await handleKindeCallback(authenticatedRequest, env);
    } else if (pathname === '/logout' && method === 'GET') { // Matches App 1 frontend redirect
        response = handleKindeLogout(authenticatedRequest, env);
    }

    // Public API Routes (No Auth Required)
    else if (pathname === '/api/settings' && method === 'GET') {
        response = await handleGetSettings(authenticatedRequest, env);
    }
    // Example: GET /api/teams/1234
    const teamByCodeRegex = /^\/api\/teams\/(\d{4})$/; // Match 4 digits
    const teamByCodeMatch = pathname.match(teamByCodeRegex);
    if (teamByCodeMatch && method === 'GET') {
         authenticatedRequest.params = { code: teamByCodeMatch[1] };
         response = await handleGetTeamByCode(authenticatedRequest, env);
    }
    // Example: GET /api/live/state/123
    const liveStateRegex = /^\/api\/live\/state\/(\d+)$/; // Match match ID
    const liveStateMatch = pathname.match(liveStateRegex);
    if (liveStateMatch && method === 'GET') {
         authenticatedRequest.params = { id: liveStateMatch[1] };
         response = await handleGetLiveState(authenticatedRequest, env);
    }
    // Example: GET /api/live/ws/123 (WebSocket Endpoint)
    const liveWsRegex = /^\/api\/live\/ws\/(\d+)$/; // Match match ID
    const liveWsMatch = pathname.match(liveWsRegex);
    if (liveWsMatch && method === 'GET') {
        const matchId = parseInt(liveWsMatch[1], 10);
        if (isNaN(matchId)) {
             response = apiError('Invalid match ID in WebSocket path', 400);
        } else {
            // Check for WebSocket Upgrade header
            if (request.headers.get('Upgrade') !== 'websocket') {
                response = new Response('Expected Upgrade: websocket', { status: 426 }); // 426 Upgrade Required
            } else {
                // Get the Durable Object ID for this match
                const doId = env.LIVE_MATCH_DO.idFromName(matchId.toString());
                // Get the Durable Object stub
                const stub = env.LIVE_MATCH_DO.get(doId);
                // Forward the request to the Durable Object
                // The DO will handle the WebSocket upgrade
                // Use a specific path like /websocket for the DO's internal routing
                response = await stub.fetch(new Request(stub.id.toString() + '/websocket', request));
            }
        }
    }


    // Authenticated API Routes (Require Kinde Auth)
    else if (pathname.startsWith('/api/')) {
        // Apply Authentication Middleware
        const authResponse = await authMiddleware(authenticatedRequest, env);

        // If middleware returned a response (e.g., 401 Unauthorized, 403 Not Registered), return it immediately
        if (authResponse) {
             // CORS headers are already added by apiError/apiResponse inside authMiddleware
             return authResponse;
        }

        // Now that the user is authenticated, handle specific API routes

        // User API Routes
        if (pathname === '/api/members/me' && method === 'GET') {
            response = await handleFetchMe(authenticatedRequest, env);
        }
        // TODO: Add other user API routes here (e.g., PATCH /api/members/me)


        // Admin API Routes (Require Admin Authentication)
        else if (pathname.startsWith('/api/admin/')) {
             // Apply Admin Authentication Middleware
             const adminAuthResponse = await adminAuthMiddleware(authenticatedRequest, env);

             // If admin middleware returned a response (e.g., 403 Forbidden), return it
             if (adminAuthResponse) {
                 // CORS headers are already added by apiError inside adminAuthMiddleware
                 return adminAuthResponse;
             }

             // Now that the user is an admin, handle specific Admin API routes

             // GET /api/admin/matches
             if (pathname === '/api/admin/matches' && method === 'GET') {
                 response = await handleAdminFetchMatches(authenticatedRequest, env);
             }
             // POST /api/admin/matches
             else if (pathname === '/api/admin/matches' && method === 'POST') {
                 response = await handleAdminCreateMatch(authenticatedRequest, env);
             }
             // GET /api/admin/matches/:id
             const adminMatchRegex = /^\/api\/admin\/matches\/(\d+)$/;
             const adminMatchMatch = pathname.match(adminMatchRegex);
             if (adminMatchMatch && method === 'GET') {
                 authenticatedRequest.params = { id: adminMatchMatch[1] };
                 response = await handleAdminGetMatch(authenticatedRequest, env);
             }
             // POST /api/admin/matches/:id/start
             const adminMatchStartRegex = /^\/api\/admin\/matches\/(\d+)\/start$/;
             const adminMatchStartMatch = pathname.match(adminMatchStartRegex);
             if (adminMatchStartMatch && method === 'POST') {
                 authenticatedRequest.params = { id: adminMatchStartMatch[1] };
                 response = await handleAdminStartMatch(authenticatedRequest, env, ctx);
             }
             // POST /api/admin/matches/:id/record-turn
             const adminMatchRecordTurnRegex = /^\/api\/admin\/matches\/(\d+)\/record-turn$/;
             const adminMatchRecordTurnMatch = pathname.match(adminMatchRecordTurnRegex);
             if (adminMatchRecordTurnMatch && method === 'POST') {
                 authenticatedRequest.params = { id: adminMatchRecordTurnMatch[1] };
                 response = await handleAdminRecordTurn(authenticatedRequest, env, ctx);
             }
             // POST /api/admin/matches/:id/end
             const adminMatchEndRegex = /^\/api\/admin\/matches\/(\d+)\/end$/;
             const adminMatchEndMatch = pathname.match(adminMatchEndRegex);
             if (adminMatchEndMatch && method === 'POST') {
                 authenticatedRequest.params = { id: adminMatchEndMatch[1] };
                 response = await handleAdminEndMatch(authenticatedRequest, env, ctx);
             }
             // GET /api/admin/matches/:id/random-song
             const adminMatchRandomSongRegex = /^\/api\/admin\/matches\/(\d+)\/random-song$/;
             const adminMatchRandomSongMatch = pathname.match(adminMatchRandomSongRegex);
             if (adminMatchRandomSongMatch && method === 'GET') {
                 authenticatedRequest.params = { id: adminMatchRandomSongMatch[1] };
                 response = await handleAdminGetRandomSong(authenticatedRequest, env);
             }
             // POST /api/admin/matches/:id/set-final-songs
             const adminMatchSetFinalSongsRegex = /^\/api\/admin\/matches\/(\d+)\/set-final-songs$/;
             const adminMatchSetFinalSongsMatch = pathname.match(adminMatchSetFinalSongsRegex);
             if (adminMatchSetFinalSongsMatch && method === 'POST') {
                 authenticatedRequest.params = { id: adminMatchSetFinalSongsMatch[1] };
                 response = await handleAdminSetFinalSongs(authenticatedRequest, env);
             }

             // TODO: Add other admin routes here...
             // else if (pathname === '/api/admin/members' && method === 'GET') { ... handleAdminFetchMembers ... }
             // else if (pathname === '/api/admin/songs' && method === 'GET') { ... handleAdminFetchSongs ... }


        }
        // If it's an /api/ route but didn't match any specific handler (after auth)
        if (!response) {
             response = apiError('API Endpoint Not Found', 404);
        }

    }
    // Handle other non-API routes here if any...
    // else if (pathname === '/') { ... handleHomepage ... }


    // If no route matched at all
    if (!response) {
        response = new Response('Not Found', { status: 404 });
    }

    // CORS headers are already added by apiResponse/apiError or manually for OPTIONS/WebSocket
    // No need to add them again here for standard responses.

    return response;
  },
};

// Export the Durable Object class so Cloudflare can find it
export { LiveMatchDO } from './durableobjects/LiveMatchDO';
