// src/index.ts

// --- Imports ---
import { createRemoteJWKSet, jwtVerify } from 'jose'; // Import jose functions
// Import your updated types
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
    SaveMemberSongPreferencePayload, // Added
    Song,
    MatchSong,
    SelectTiebreakerSongPayload,
    RoundSummary,
    SongLevel,
    ApiResponse,
    SongsApiResponseData,
    SongFiltersApiResponseData,
    PaginationInfo,
    KindeUser // Added
} from './types'; // Adjust path to your types file
import { MatchDO } from './durable-objects/matchDo'; // Adjust path to your DO file

// --- Configuration & Constants ---
const CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*', // Or restrict to your frontend domain
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS', // Added PATCH
    'Access-Control-Allow-Headers': 'Content-Type, Authorization', // Include Authorization header
    'Access-Control-Max-Age': '86400', // Cache preflight requests for 24 hours
    'Access-Control-Allow-Credentials': 'true', // IMPORTANT for cookies
};

// Define JWKS outside the fetch handler to reuse the connection and cache keys
let kindeJwks: ReturnType<typeof createRemoteJWKSet> | undefined;

// --- Helper Functions (Keep existing helpers like getMatchDO, forwardRequestToDO, jsonResponse, errorResponse, etc.) ---

// Helper to get a DO instance by Name string (using idFromName)
const getMatchDO = (doName: string, env: Env): DurableObjectStub => {
    const id: DurableObjectId = env.MATCH_DO.idFromName(doName);
    return env.MATCH_DO.get(id);
};

// Helper to handle forwarding requests to DOs
// Modified to read body inside if needed, to avoid middleware consuming it
const forwardRequestToDO = async (doIdString: string, env: Env, request: Request, internalPath: string, method: string = 'POST'): Promise<Response> => {
    try {
        const doStub = getMatchDO(doIdString, env);
        const doUrl = new URL(`https://dummy-host`); // Dummy host is fine for DO fetch
        doUrl.pathname = internalPath;

        const newHeaders = new Headers();
        for (const [key, value] of request.headers.entries()) {
            if (key.toLowerCase() !== 'host') {
                newHeaders.append(key, value);
            }
        }

        let requestBody: BodyInit | null | undefined = undefined;
        // Only attempt to read body for methods that typically have one
        if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
             try {
                 // Clone the request before reading the body, in case the original is needed elsewhere
                 const clonedRequest = request.clone();
                 // Read the body as text first to handle potential non-JSON bodies or errors
                 const bodyText = await clonedRequest.text();
                 if (bodyText) {
                     // Attempt to parse as JSON, but send as text if parsing fails or it's not JSON
                     try {
                         const bodyJson = JSON.parse(bodyText);
                         requestBody = JSON.stringify(bodyJson); // Send as JSON string
                         newHeaders.set('Content-Type', 'application/json'); // Ensure correct content type
                     } catch (e) {
                         // If JSON parsing fails, send the body as text
                         requestBody = bodyText;
                         // Attempt to preserve original Content-Type if it wasn't JSON
                         if (!newHeaders.has('Content-Type') || newHeaders.get('Content-Type')?.toLowerCase() === 'application/json') {
                             newHeaders.set('Content-Type', 'text/plain'); // Default to text if original was JSON or missing
                         }
                         console.warn(`Worker: Failed to parse request body as JSON for DO forwarding to ${internalPath}. Sending as text.`, e);
                     }
                 }
             } catch (e) {
                 console.error(`Worker: Failed to read request body for DO forwarding to ${internalPath}:`, e);
                 // Continue without body if reading fails
             }
        }


        const requestInit: RequestInit = {
            method: method,
            headers: newHeaders,
            body: requestBody,
            redirect: 'follow',
            cf: request.cf,
        };

        // Create the Request object specifically for the DO fetch
        const doRequest = new Request(doUrl.toString(), requestInit);

        console.log(`Worker: Forwarding ${method} to DO ${doIdString} at path ${internalPath}`);
        const response = await doStub.fetch(doRequest); // Use the created request
        console.log(`Worker: Received response from DO ${doIdString} for path ${internalPath}. Status: ${response.status}`);

        return response;

    } catch (e: any) {
        console.error(`Worker: Failed to forward request to DO ${doIdString} for path ${internalPath}:`, e);
        console.error(`Worker: Error details - Name: ${e.name}, Message: ${e.message}`);
        if (e.stack) {
            console.error(`Worker: Error stack: ${e.stack}`);
        }
        return new Response(JSON.stringify({ success: false, error: `Failed to communicate with match instance: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
};


// Helper to wrap responses in ApiResponse format (Keep as is)
function jsonResponse<T>(data: T, status: number = 200): Response {
    return new Response(JSON.stringify({ success: true, data }), {
        status,
        headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
    });
}

function errorResponse(error: string, status: number = 500): Response {
    console.error(`API Error (${status}): ${error}`); // Log errors on the backend
    return new Response(JSON.stringify({ success: false, error }), {
        status,
        headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
    });
}


// Placeholder for R2 Avatar Upload (Ensure this uses env.AVATAR_BUCKET and env.R2_PUBLIC_BUCKET_URL)
async function uploadAvatar(env: Env, file: File, identifier: string, teamCode: string): Promise<string | null> {
    console.log(`Uploading avatar for ${identifier} in team ${teamCode}`);
    try {
        const fileExtension = file.name.split('.').pop()?.toLowerCase() || 'jpg';
        // Use Kinde User ID or Maimai ID for uniqueness
        const uniqueIdentifier = identifier.replace(/[^a-zA-Z0-9_-]/g, ''); // Sanitize identifier
        const objectKey = `avatars/${teamCode}/${uniqueIdentifier}_${Date.now()}.${fileExtension}`;
        const uploadResult = await env.AVATAR_BUCKET.put(objectKey, file.stream());

        // Assuming your R2 bucket has a public access URL configured
        if (!env.R2_PUBLIC_BUCKET_URL) {
             console.error("R2_PUBLIC_BUCKET_URL environment variable is not set.");
             return null;
        }
        return `${env.R2_PUBLIC_BUCKET_URL}/${objectKey}`;
    } catch (e) {
        console.error("R2 upload failed:", e);
        return null;
    }
}

// Placeholder for R2 Avatar Deletion (Ensure this uses env.AVATAR_BUCKET and env.R2_PUBLIC_BUCKET_URL)
async function deleteAvatarFromR2(env: Env, url: string): Promise<void> {
    console.log(`Deleting avatar from R2: ${url}`);
    try {
        if (!env.R2_PUBLIC_BUCKET_URL) {
             console.warn("R2_PUBLIC_BUCKET_URL environment variable is not set. Cannot delete avatar.");
             return;
        }
        // Extract the object key from the URL
        if (url.startsWith(env.R2_PUBLIC_BUCKET_URL)) {
            const objectKey = url.substring(env.R2_PUBLIC_BUCKET_URL.length + 1); // +1 for the leading slash
            console.log(`Deleting R2 object with key: ${objectKey}`);
            await env.AVATAR_BUCKET.delete(objectKey);
            console.log(`R2 object ${objectKey} deleted.`);
        } else {
            console.warn(`Avatar URL does not match R2 public URL base (${env.R2_PUBLIC_BUCKET_URL}), skipping deletion: ${url}`);
        }
    } catch (e) {
        console.error("R2 deletion failed:", e);
        // Log the error but don't throw, deletion is best effort
    }
}

// Placeholder for checking and deleting empty teams (Keep as is)
async function checkAndDeleteEmptyTeam(env: Env, teamCode: string): Promise<void> {
    // ... (Your existing checkAndDeleteEmptyTeam logic) ...
    console.log(`Checking if team ${teamCode} is empty for deletion.`);
    try {
        const countResult = await env.DB.prepare('SELECT COUNT(*) as count FROM members WHERE team_code = ?').bind(teamCode).first<{ count: number }>();
        const memberCount = countResult?.count ?? 0;

        if (memberCount === 0) {
            console.log(`Team ${teamCode} is empty. Deleting team record.`);
            const deleteResult = await env.DB.prepare('DELETE FROM teams WHERE code = ?').bind(teamCode).run();
            if (deleteResult.success) {
                console.log(`Team ${teamCode} deleted successfully.`);
            } else {
                console.error(`Failed to delete empty team ${teamCode}:`, deleteResult.error);
            }
        } else {
            console.log(`Team ${teamCode} is not empty (${memberCount} members). Not deleting.`);
        }
    } catch (e) {
        console.error(`Error checking/deleting empty team ${teamCode}:`, e);
    }
}

// Helper functions for CSV export (reused from frontend or defined here) (Keep as is)
// ... (Your existing getColorText, getJobText, formatTimestamp) ...
function getColorText(colorId: string | null | undefined): string {
     const map: { [key: string]: string } = { red: '火', green: '木', blue: '水' };
     return map[colorId || ''] || '未知';
}

function getJobText(jobType: string | null | undefined): string {
    const map: { [key: string]: string } = { attacker: '绝剑士', defender: '矩盾手', supporter: '炼星师' };
    return map[jobType || ''] || '未知';
}

function formatTimestamp(timestamp: number | null | undefined): string {
    if (!timestamp) return 'N/A';
    try {
        const date = new Date(timestamp * 1000); // Convert seconds to milliseconds
        const year = date.getFullYear();
        const month = ('0' + (date.getMonth() + 1)).slice(-2);
        const day = ('0' + date.getDate()).slice(-2);
        const hours = ('0' + date.getHours()).slice(-2);
        const minutes = ('0' + date.getMinutes()).slice(-2);
        const seconds = ('0' + date.getSeconds()).slice(-2);
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (e) {
        console.error("Failed to format timestamp:", timestamp, e);
        return 'Invalid Date';
    }
}


// --- Kinde Authentication Helpers ---

// Helper to verify Kinde Access Token using jose (Keep as is, looks correct)
async function verifyKindeToken(env: Env, token: string): Promise<{ userId: string, claims: any } | null> {
    if (!env.KINDE_ISSUER_URL) {
        console.error("KINDE_ISSUER_URL not configured in Worker secrets.");
        return null;
    }

    if (!kindeJwks) {
        try {
             kindeJwks = createRemoteJWKSet(new URL(`${env.KINDE_ISSUER_URL}/.well-known/jwks`));
             console.log("Kinde JWKS set created.");
        } catch (e) {
             console.error("Failed to create Kinde JWKS set:", e);
             return null;
        }
    }

    try {
        const { payload } = await jwtVerify(token, kindeJwks, {
            issuer: env.KINDE_ISSUER_URL,
            // audience: env.KINDE_AUDIENCE, // If you have an audience
        });

        if (!payload.sub) {
             console.error("Kinde token payload missing 'sub' claim.");
             return null;
        }

        return { userId: payload.sub, claims: payload };

    } catch (e) {
        console.error("Error verifying Kinde token with jose:", e);
        return null;
    }
}

// Middleware-like function to extract Kinde User ID from token/cookie (Keep as is, looks correct)
async function getAuthenticatedKindeUser(request: Request, env: Env): Promise<string | null> {
    const authHeader = request.headers.get('Authorization');
    let token = null;
    if (authHeader?.startsWith('Bearer ')) {
        token = authHeader.substring(7);
    } else {
        const cookieHeader = request.headers.get('Cookie');
        if (cookieHeader) {
            const cookies = cookieHeader.split(';').map(c => c.trim().split('='));
            const accessTokenCookie = cookies.find(cookie => cookie[0] === 'kinde_access_token');
            if (accessTokenCookie) {
                token = accessTokenCookie[1];
            }
        }
    }

    if (!token) {
        return null; // No token found
    }

    const verificationResult = await verifyKindeToken(env, token);
    if (!verificationResult) {
        console.warn("Kinde token verification failed.");
        return null; // Token invalid or expired
    }

    return verificationResult.userId; // Return the Kinde user ID
}

// Helper to check if the authenticated Kinde user is marked as admin in the DB (Keep as is, looks correct)
async function isAdminUser(env: Env, kindeUserId: string): Promise<boolean> {
    if (!kindeUserId) return false;
    try {
        const member = await env.DB.prepare('SELECT is_admin FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ is_admin: number | null }>();
        return member?.is_admin === 1;
    } catch (e) {
        console.error(`Database error checking admin status for Kinde ID ${kindeUserId}:`, e);
        return false; // Assume not admin on error
    }
}

// --- Collection Status Helper ---
async function isCollectionPaused(env: Env): Promise<boolean> {
    try {
        const setting = await env.DB.prepare('SELECT value FROM settings WHERE key = ? LIMIT 1')
            .bind('collection_paused')
            .first<{ value: string }>();
        return setting?.value === 'true';
    } catch (e) {
        console.error('Database error fetching collection_paused setting:', e);
        return false;
    }
}


// --- Authentication Middleware ---
// This middleware checks if the user is authenticated via Kinde token/cookie.
// If authenticated, it calls the next handler with the kindeUserId.
// If not authenticated, it returns a 401 Unauthorized response.
type AuthenticatedHandler = (request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string) => Promise<Response>;

async function authMiddleware(request: Request, env: Env, ctx: ExecutionContext, handler: AuthenticatedHandler): Promise<Response> {
    const kindeUserId = await getAuthenticatedKindeUser(request, env);

    if (!kindeUserId) {
        console.warn(`Authentication required for ${new URL(request.url).pathname}`);
        return apiError('Authentication required.', 401);
    }

    // User is authenticated, proceed to the actual handler with the user ID
    return handler(request, env, ctx, kindeUserId);
}

// --- Admin Authentication Middleware ---
// This middleware checks if the user is authenticated AND is an admin.
type AdminAuthenticatedHandler = (request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string) => Promise<Response>;

async function adminAuthMiddleware(request: Request, env: Env, ctx: ExecutionContext, handler: AdminAuthenticatedHandler): Promise<Response> {
    // First, check if the user is authenticated at all
    const kindeUserId = await getAuthenticatedKindeUser(request, env);

    if (!kindeUserId) {
        console.warn(`Admin access denied: User not authenticated via Kinde for ${new URL(request.url).pathname}`);
        return apiError('Authentication required.', 401);
    }

    // If authenticated, check if they are an admin
    const isAdmin = await isAdminUser(env, kindeUserId);
    if (!isAdmin) {
        console.warn(`Admin access denied: User ${kindeUserId} is not an admin for ${new URL(request.url).pathname}`);
        return apiError('Authorization failed: You do not have administrator privileges.', 403);
    }

    // If authenticated and is admin, proceed to the actual admin handler
    return handler(request, env, ctx, kindeUserId);
}


// --- Route Handlers ---

// GET /api/settings (Public) (Keep as is)
async function handleGetSettings(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/settings request...');
    try {
        const paused = await isCollectionPaused(env);
        return jsonResponse({ collection_paused: paused }, 200);
    } catch (e) {
        console.error('Error fetching settings:', e);
        return errorResponse('Failed to fetch settings.', 500, e);
    }
}

// POST /api/kinde/callback (Public) - NEW
async function handleKindeCallback(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    console.log('Handling /api/kinde/callback request...');
    const url = new URL(request.url);
    const body = await request.json().catch(() => null);
    if (!body) return errorResponse('Invalid or missing JSON body.', 400);

    const { code, code_verifier, redirect_uri } = body;

    if (!code || !code_verifier || !redirect_uri) {
        return errorResponse('Missing code, code_verifier, or redirect_uri in callback request.', 400);
    }

    if (!env.KINDE_CLIENT_ID || !env.KINDE_CLIENT_SECRET || !env.KINDE_ISSUER_URL || !env.KINDE_REDIRECT_URI || !env.LOGOUT_REDIRECT_TARGET_URL) {
         console.error("Kinde secrets not configured in Worker.");
         return errorResponse('Server configuration error.', 500);
    }

    try {
        const tokenUrl = `${env.KINDE_ISSUER_URL}/oauth2/token`;
        const tokenResponse = await fetch(tokenUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: new URLSearchParams({
                client_id: env.KINDE_CLIENT_ID,
                client_secret: env.KINDE_CLIENT_SECRET,
                code: code,
                code_verifier: code_verifier,
                grant_type: 'authorization_code',
                redirect_uri: redirect_uri,
            }),
        });

        const tokenData = await tokenResponse.json();

        if (!tokenResponse.ok) {
            console.error('Kinde token exchange failed:', tokenResponse.status, tokenData);
            return errorResponse(tokenData.error_description || tokenData.error || 'Failed to exchange authorization code for tokens.', tokenResponse.status);
        }

        const { access_token, id_token, refresh_token, expires_in } = tokenData;

        const headers = new Headers(CORS_HEADERS);
        const secure = url.protocol === 'https:' ? '; Secure' : '';
        const domain = url.hostname; // Use the domain from the request URL for the cookie domain

        // Set Access Token cookie (HttpOnly)
        headers.append('Set-Cookie', `kinde_access_token=${access_token}; HttpOnly; Path=/; Max-Age=${expires_in}; SameSite=Lax${secure}; Domain=${domain}`);

        // Set Refresh Token cookie (HttpOnly)
        if (refresh_token) {
             const refreshTokenMaxAge = 30 * 24 * 60 * 60; // 30 days
             headers.append('Set-Cookie', `kinde_refresh_token=${refresh_token}; HttpOnly; Path=/; Max-Age=${refreshTokenMaxAge}; SameSite=Lax${secure}; Domain=${domain}`);
        }

        // Decode ID token for basic user info to return to frontend
        let userInfo: KindeUser | {} = {};
        try {
            const idTokenPayload = JSON.parse(atob(id_token.split('.')[1]));
            userInfo = {
                id: idTokenPayload.sub, // Kinde User ID
                email: idTokenPayload.email,
                name: idTokenPayload.given_name && idTokenPayload.family_name ? `${idTokenPayload.given_name} ${idTokenPayload.family_name}` : idTokenPayload.given_name || idTokenPayload.family_name || idTokenPayload.email,
            } as KindeUser;
        } catch (e) {
            console.error("Failed to decode ID token payload:", e);
        }

        return new Response(JSON.stringify({ success: true, data: { user: userInfo } }), { // Wrap user in data object for ApiResponse format
            status: 200,
            headers: headers,
        });

    } catch (kindeError) {
        console.error('Error during Kinde token exchange:', kindeError);
        return errorResponse('Failed to communicate with authentication server.', 500, kindeError);
    }
}

// NEW: GET /api/logout (Public) - Handles clearing HttpOnly cookies and redirect
async function handleLogout(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/logout request...');
    const url = new URL(request.url);
    const headers = new Headers(CORS_HEADERS);
    const secure = url.protocol === 'https:' ? '; Secure' : '';
    const domain = url.hostname;

    // Clear Access Token cookie
    headers.append('Set-Cookie', `kinde_access_token=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax${secure}; Domain=${domain}`);
    // Clear Refresh Token cookie
    headers.append('Set-Cookie', `kinde_refresh_token=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax${secure}; Domain=${domain}`);

    // Redirect to Kinde's logout endpoint
    if (!env.KINDE_ISSUER_URL || !env.LOGOUT_REDIRECT_TARGET_URL) {
         console.error("Kinde logout configuration missing.");
         // If config is missing, just return a success response after clearing cookies
         return new Response(JSON.stringify({ success: true, message: "Logged out (cookies cleared)." }), { status: 200, headers });
    }

    const kindeLogoutUrl = new URL(`${env.KINDE_ISSUER_URL}/logout`);
    kindeLogoutUrl.searchParams.append('redirect', env.LOGOUT_REDIRECT_TARGET_URL);

    // Return a redirect response
    return Response.redirect(kindeLogoutUrl.toString(), 302);
}


// GET /api/teams (Public) (Keep as is)
async function handleFetchTeams(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/teams request...');
    try {
        const { results } = await env.DB.prepare("SELECT * FROM teams").all<Team>();
        return jsonResponse(results);
    } catch (e: any) {
        console.error("Worker: Failed to list teams:", e);
        return errorResponse(e.message);
    }
}

// GET /api/teams/:code (Public) (Keep as is)
async function handleGetTeamByCode(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/teams/:code request...');
    const parts = new URL(request.url).pathname.split('/');
    if (parts.length !== 4 || !parts[3]) {
        return errorResponse('Invalid API path. Use /api/teams/:code', 400);
    }
    const teamCode = parts[3];

    if (teamCode.length !== 4 || isNaN(parseInt(teamCode))) {
        return errorResponse('Invalid team code format.', 400);
    }

    try {
        const teamResult = await env.DB.prepare('SELECT name FROM teams WHERE code = ? LIMIT 1').bind(teamCode).first<{ name: string }>();

        if (!teamResult) {
            return errorResponse(`Team with code ${teamCode} not found.`, 404);
        }

        const membersResult = await env.DB.prepare(
            'SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE team_code = ? ORDER BY joined_at ASC'
        ).bind(teamCode).all<Member>(); // Use Member type

        return jsonResponse({
            success: true, // Add success: true for consistency with ApiResponse
            code: teamCode,
            name: teamResult.name,
            members: membersResult.results || []
        }, 200);

    } catch (e: any) {
        console.error('Database error fetching team by code:', e);
        return errorResponse('Failed to fetch team information.', 500, e);
    }
}

// POST /api/teams/check (Public) (Keep as is)
async function handleCheckTeam(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/teams/check request...');
    const paused = await isCollectionPaused(env);
    if (paused) {
        console.log('Collection is paused. Denying team check.');
        return errorResponse('现在的组队已停止，如需更多信息，请访问官网或咨询管理员。', 403);
    }

    const body = await request.json().catch(() => null);
    if (!body || typeof body.teamCode !== 'string') {
        return errorResponse('Invalid or missing teamCode in request body.', 400);
    }
    const teamCode = body.teamCode.trim();

    if (teamCode.length !== 4 || isNaN(parseInt(teamCode))) {
        return errorResponse('Invalid team code format.', 400);
    }

    try {
        const teamResult = await env.DB.prepare('SELECT name FROM teams WHERE code = ? LIMIT 1').bind(teamCode).first<{ name: string }>();

        if (!teamResult) {
            return errorResponse(`Team with code ${teamCode} not found.`, 404);
        }

        const membersResult = await env.DB.prepare(
            'SELECT color, job, maimai_id, nickname, avatar_url FROM members WHERE team_code = ?'
        ).bind(teamCode).all<Partial<Member>>(); // Use Partial<Member> as not all fields are selected

        return jsonResponse({
            success: true,
            code: teamCode,
            name: teamResult.name,
            members: membersResult.results || []
        }, 200);

    } catch (e: any) {
        console.error('Database error checking team:', e);
        return errorResponse('Failed to check team information.', 500, e);
    }
}

// POST /api/teams/create (Public) (Keep as is)
async function handleCreateTeam(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/teams/create request...');
    const paused = await isCollectionPaused(env);
    if (paused) {
        console.log('Collection is paused. Denying team creation.');
        return errorResponse('现在的组队已停止，如需更多信息，请访问官网或咨询管理员。', 403);
    }

    const body = await request.json().catch(() => null);
    if (!body || typeof body.teamCode !== 'string' || typeof body.teamName !== 'string') {
        return errorResponse('Invalid or missing teamCode or teamName in request body.', 400);
    }
    const teamCode = body.teamCode.trim();
    const teamName = body.teamName.trim();

    if (teamCode.length !== 4 || isNaN(parseInt(teamCode))) {
        return errorResponse('Invalid team code format.', 400);
    }
    if (teamName.length === 0 || teamName.length > 50) {
        return errorResponse('Team name is required (1-50 chars).', 400);
    }

    try {
        const existingTeam = await env.DB.prepare('SELECT 1 FROM teams WHERE code = ? LIMIT 1').bind(teamCode).first();
        if (existingTeam) {
            return errorResponse(`Team code ${teamCode} already exists.`, 409);
        }

        const now = Math.floor(Date.now() / 1000);
        const insertResult = await env.DB.prepare(
            'INSERT INTO teams (code, name, created_at) VALUES (?, ?, ?)'
        )
        .bind(teamCode, teamName, now)
        .run();

        if (!insertResult.success) {
            console.error('Create team database insert failed:', insertResult.error);
            return errorResponse('Failed to create team due to a database issue.', 500);
        }

        return jsonResponse({ success: true, message: "Team created successfully.", code: teamCode, name: teamName }, 201);

    } catch (e: any) {
        console.error('Database error creating team:', e);
        return errorResponse('Failed to create team.', 500, e);
    }
}


// GET /api/members (Public - Can filter by team_code) (Keep as is)
async function handleFetchMembers(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/members request...');
    try {
        const url = new URL(request.url);
        const teamCode = url.searchParams.get('team_code');
        let query = "SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members"; // Select all relevant fields
        let params: string[] = [];
        if (teamCode) {
            query += " WHERE team_code = ?";
            params.push(teamCode);
        }
        const { results } = await env.DB.prepare(query).bind(...params).all<Member>(); // Use Member type
        return jsonResponse(results);
    } catch (e: any) {
        console.error("Worker: Failed to list members:", e);
        return errorResponse(e.message);
    }
}

// GET /api/members/:id (Public) (Keep as is)
async function handleGetMemberById(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/members/:id request...');
    const parts = new URL(request.url).pathname.split('/');
    const memberId = parseInt(parts[3], 10);
    if (isNaN(memberId)) {
        return errorResponse("Invalid member ID in path", 400);
    }
    try {
        const member = await env.DB.prepare("SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?").bind(memberId).first<Member>(); // Use Member type
        if (member) {
            return jsonResponse(member);
        } else {
            return errorResponse("Member not found", 404);
        }
    } catch (e: any) {
        console.error(`Worker: Failed to get member ${memberId}:`, e);
        return errorResponse(e.message);
    }
}


// GET /api/members/me (Authenticated User) - NEW
async function handleFetchMe(request: Request, env: Env, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/members/me request for Kinde user ID: ${kindeUserId}`);
    try {
        // Find the member record associated with this Kinde User ID
        const member = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE kinde_user_id = ?')
            .bind(kindeUserId)
            .first<Member>(); // Use Member type

        if (!member) {
            // Return success: true with null data if user is authenticated but not registered
            return jsonResponse({ member: null, message: "User not registered." }, 200);
        }

        return jsonResponse({ member: member }, 200); // Wrap member in data object for ApiResponse format

    } catch (e: any) {
        console.error(`Database error fetching member for Kinde ID ${kindeUserId}:`, e);
        return errorResponse('Failed to fetch member information.', 500, e);
    }
}


// POST /api/teams/join (Authenticated User) - MODIFIED to use kindeUserId
async function handleJoinTeam(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/teams/join request for Kinde user ID: ${kindeUserId}`);
    const paused = await isCollectionPaused(env);
    if (paused) {
        console.log('Collection is paused. Denying join.');
        return errorResponse('现在的组队已停止，如需更多信息，请访问官网或咨询管理员。', 403);
    }

    let formData: FormData;
    try { formData = await request.formData(); } catch (e) { return errorResponse('Invalid request format. Expected multipart/form-data.', 400, e); }

    const teamCode = formData.get('teamCode')?.toString();
    const color = formData.get('color')?.toString();
    const job = formData.get('job')?.toString();
    const maimaiId = formData.get('maimaiId')?.toString()?.trim();
    const nickname = formData.get('nickname')?.toString()?.trim();
    const qqNumber = formData.get('qqNumber')?.toString()?.trim();
    const avatarFile = formData.get('avatarFile');

     // --- Input Validation ---
     if (!teamCode || teamCode.length !== 4 || isNaN(parseInt(teamCode))) return errorResponse('Invalid team code.', 400);
     if (!color || !['red', 'green', 'blue'].includes(color)) return errorResponse('Invalid color selection.', 400);
     if (!job || !['attacker', 'defender', 'supporter'].includes(job)) return errorResponse('Invalid job selection.', 400);
     if (!maimaiId || maimaiId.length === 0 || maimaiId.length > 13) return errorResponse('Maimai ID is required (1-13 chars).', 400);
     if (!nickname || nickname.length === 0 || nickname.length > 50) return errorResponse('Nickname is required (1-50 chars).', 400);
     if (!qqNumber || !/^[1-9][0-9]{4,14}$/.test(qqNumber)) return errorResponse('A valid QQ number is required.', 400);
     // --- End Validation ---

    try {
         const teamChecks = await env.DB.batch([
             env.DB.prepare('SELECT name FROM teams WHERE code = ? LIMIT 1').bind(teamCode),
             env.DB.prepare('SELECT COUNT(*) as count FROM members WHERE team_code = ?').bind(teamCode),
             // Check if THIS Kinde user ID already has a registration
             env.DB.prepare('SELECT 1 FROM members WHERE kinde_user_id = ? LIMIT 1').bind(kindeUserId),
             // Check if color or job is already taken in this specific team
             env.DB.prepare('SELECT 1 FROM members WHERE team_code = ? AND (color = ? OR job = ?) LIMIT 1').bind(teamCode, color, job),
         ]);

         const teamResult = teamChecks[0]?.results?.[0] as { name: string } | undefined;
         const memberCount = (teamChecks[1]?.results?.[0] as { count: number } | undefined)?.count ?? 0;
         const existingMemberWithKindeId = teamChecks[2]?.results?.[0];
         const conflictCheck = teamChecks[3]?.results?.[0];

         if (!teamResult) return errorResponse(`Team with code ${teamCode} not found.`, 404);
         if (memberCount >= 3) return errorResponse(`Team ${teamCode} is already full (3 members).`, 409);
         if (existingMemberWithKindeId) {
             return errorResponse('你已经报名过了，一个账号只能报名一次。', 409);
         }

         if (conflictCheck) {
             const colorConflict = await env.DB.prepare('SELECT 1 FROM members WHERE team_code = ? AND color = ? LIMIT 1').bind(teamCode, color).first();
             if (colorConflict) return errorResponse(`The color '${color}' is already taken in team ${teamCode}.`, 409);
             const jobConflict = await env.DB.prepare('SELECT 1 FROM members WHERE team_code = ? AND job = ? LIMIT 1').bind(teamCode, job).first();
             if (jobConflict) return errorResponse(`The job '${job}' is already taken in team ${teamCode}.`, 409);
             // Fallback generic conflict message if both checks pass but the batch check failed (shouldn't happen)
             return errorResponse(`Color or job is already taken in team ${teamCode}.`, 409);
         }

         let avatarUrl: string | null = null;
         if (avatarFile instanceof File) {
              // Use Kinde User ID in avatar path for better uniqueness and association
              avatarUrl = await uploadAvatar(env, avatarFile, kindeUserId, teamCode); // Use kindeUserId
               if (avatarUrl === null) {
                  console.warn(`Join blocked for ${maimaiId}: Avatar upload failed.`);
                  return errorResponse('Failed to upload avatar. Member not added.', 500);
               }
         }

         const now = Math.floor(Date.now() / 1000);
         const insertResult = await env.DB.prepare(
             'INSERT INTO members (team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, kinde_user_id, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
         )
         // Default is_admin to 0 for user signups
         .bind(teamCode, color, job, maimaiId, nickname, qqNumber, avatarUrl, now, kindeUserId, 0)
         .run();

         if (!insertResult.success) {
             console.error('Join team database insert failed:', insertResult.error);
             return errorResponse(insertResult.error || 'Failed to add member due to a database issue.', 500);
         }

         const newMemberId = insertResult.meta.last_row_id;
         const newMember = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?')
             .bind(newMemberId)
             .first<Member>(); // Use Member type

         return jsonResponse({ success: true, message: "Member added successfully.", member: newMember }, 201);

    } catch (processingError) {
        console.error('Error during join team processing pipeline:', processingError);
        return errorResponse(
             `Failed to process join request: ${processingError instanceof Error ? processingError.message : 'Unknown error'}`,
             500,
             processingError
        );
    }
}

// PATCH /api/members/:maimaiId (Authenticated User) - MODIFIED to use kindeUserId for auth
async function handleUserPatchMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
     console.log(`Handling /api/members/:maimaiId PATCH request for Kinde user ID: ${kindeUserId}`);
     const parts = new URL(request.url).pathname.split('/');
     if (parts.length !== 4 || !parts[3]) {
         return errorResponse('Invalid API path. Use /api/members/:maimaiId', 400);
     }
     const targetMaimaiId = parts[3]; // The Maimai ID from the URL path (used to find the record)

    let formData: FormData;
    try { formData = await request.formData(); } catch (e) { return errorResponse('Invalid request format for update. Expected multipart/form-data.', 400, e); }

    // --- Authorization Step ---
    // Verify the member exists AND belongs to the authenticated Kinde user
    const existingMember = await env.DB.prepare('SELECT * FROM members WHERE maimai_id = ? AND kinde_user_id = ?')
        .bind(targetMaimaiId, kindeUserId)
        .first<Member>(); // Use Member type

    if (!existingMember) {
        return errorResponse('Authorization failed: Member not found or does not belong to your account.', 403);
    }

    // --- Prepare Updates ---
    const updates: Partial<Member> = {};
    const setClauses: string[] = [];
    const params: (string | number | null)[] = [];
    let newAvatarUrl: string | null | undefined = undefined;
    let oldAvatarUrlToDelete: string | null | undefined = undefined;

    // Get potential new values from FormData
    const newNickname = formData.get('nickname')?.toString()?.trim();
    const newQqNumber = formData.get('qqNumber')?.toString()?.trim();
    const newColor = formData.get('color')?.toString();
    const newJob = formData.get('job')?.toString();
    const newAvatarFile = formData.get('avatarFile');
    const clearAvatar = formData.get('clearAvatar')?.toString() === 'true';

    // --- Validate and Add Fields to Update ---
    if (newNickname !== undefined && newNickname !== existingMember.nickname) {
        if (newNickname.length === 0 || newNickname.length > 50) { return errorResponse('Nickname must be between 1 and 50 characters.', 400); }
        updates.nickname = newNickname; setClauses.push('nickname = ?'); params.push(newNickname);
    }
    if (newQqNumber !== undefined && newQqNumber !== existingMember.qq_number) {
        if (!/^[1-9][0-9]{4,14}$/.test(newQqNumber)) { return errorResponse('Invalid format for new QQ number.', 400); }
        updates.qq_number = newQqNumber; setClauses.push('qq_number = ?'); params.push(newQqNumber);
    }
    // Check Color Change and Conflict (in the member's current team)
    if (newColor !== undefined && newColor !== existingMember.color) {
        if (!['red', 'green', 'blue'].includes(newColor)) return errorResponse('Invalid new color selection.', 400);
        const conflictCheck = await env.DB.prepare(
                'SELECT 1 FROM members WHERE team_code = ? AND color = ? AND id != ? LIMIT 1'
            )
            .bind(existingMember.team_code, newColor, existingMember.id)
            .first();
        if (conflictCheck) { return errorResponse(`The color '${newColor}' is already taken by another member in your team.`, 409); }
        updates.color = newColor; setClauses.push('color = ?'); params.push(newColor);
    }
   // Check Job Change and Conflict (in the member's current team)
   if (newJob !== undefined && newJob !== existingMember.job) {
        if (!['attacker', 'defender', 'supporter'].includes(newJob)) return errorResponse('Invalid new job selection.', 400);
         const conflictCheck = await env.DB.prepare(
                 'SELECT 1 FROM members WHERE team_code = ? AND job = ? AND id != ? LIMIT 1'
            )
            .bind(existingMember.team_code, newJob, existingMember.id)
            .first();
        if (conflictCheck) { return errorResponse(`The job '${newJob}' is already taken by another member in your team.`, 409); }
        updates.job = newJob; setClauses.push('job = ?'); params.push(newJob);
   }

    // --- Handle Avatar Changes ---
    if (clearAvatar) {
         newAvatarUrl = null; updates.avatar_url = null;
         if (existingMember.avatar_url) { oldAvatarUrlToDelete = existingMember.avatar_url; }
    } else if (newAvatarFile instanceof File) {
       console.log(`Processing new avatar file upload for member ID ${existingMember.id}`);
       // Use Kinde User ID in avatar path
       const idForAvatarPath = existingMember.kinde_user_id || existingMember.maimai_id; // Use Kinde ID if available
       if (!idForAvatarPath) {
            console.error(`Cannot determine identifier for avatar path for member ID ${existingMember.id}`);
            return errorResponse('Failed to determine avatar identifier.', 500);
       }
       const uploadedUrl = await uploadAvatar(env, newAvatarFile, idForAvatarPath, existingMember.team_code);
       if (uploadedUrl === null) { return errorResponse('Avatar upload failed. Profile update cancelled.', 500); }
       newAvatarUrl = uploadedUrl; updates.avatar_url = newAvatarUrl;
       if (existingMember.avatar_url && existingMember.avatar_url !== newAvatarUrl) { oldAvatarUrlToDelete = existingMember.avatar_url; }
    }
    if (newAvatarUrl !== undefined) { setClauses.push('avatar_url = ?'); params.push(newAvatarUrl); }


   if (setClauses.length === 0) { return jsonResponse({ message: "No changes detected.", member: existingMember }, 200); }

   const now = Math.floor(Date.now() / 1000);
   setClauses.push('updated_at = ?'); params.push(now);

   params.push(existingMember.id); // Use internal ID for WHERE clause

   const updateQuery = `UPDATE members SET ${setClauses.join(', ')} WHERE id = ?`;
   console.log(`Executing user update for ID ${existingMember.id}: ${updateQuery} with params: ${JSON.stringify(params.slice(0, -1))}`);

   try {
       if (oldAvatarUrlToDelete) { ctx.waitUntil(deleteAvatarFromR2(env, oldAvatarUrlToDelete)); }

        const updateResult = await env.DB.prepare(updateQuery).bind(...params).run();

        if (!updateResult.success) {
             console.error(`User update member database operation failed for ID ${existingMember.id}:`, updateResult.error);
              if (updateResult.error?.includes('UNIQUE constraint failed')) {
                   return errorResponse(`Update failed due to a conflict (color or job in team). Please check values.`, 409);
              }
             return errorResponse('Failed to update member information due to a database issue.', 500);
        }

        if (updateResult.meta.changes === 0) {
            console.warn(`User update query executed for ID ${existingMember.id} but no rows were changed.`);
            const checkExists = await env.DB.prepare('SELECT 1 FROM members WHERE id = ?').bind(existingMember.id).first();
            if (!checkExists) return errorResponse('Failed to update: Member record not found.', 404);
            return jsonResponse({ message: "No changes detected or record unchanged.", member: existingMember }, 200);
        }

        console.log(`Successfully updated member ID ${existingMember.id}. Changes: ${updateResult.meta.changes}`);

        // Fetch the *updated* member data to return
        const updatedMember = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?')
            .bind(existingMember.id)
            .first<Member>(); // Use Member type

        if (!updatedMember) {
              console.error(`Consistency issue: Member ID ${existingMember.id} updated but could not be re-fetched.`);
              return errorResponse('Update successful, but failed to retrieve updated data.', 500);
        }

        return jsonResponse({ success: true, message: "Information updated successfully.", member: updatedMember }, 200);

   } catch (updateProcessError) {
        console.error(`Error during the user member update process for ID ${existingMember.id}:`, updateProcessError);
        return errorResponse(
             `Failed to process update: ${updateProcessError instanceof Error ? updateProcessError.message : 'Unknown error'}`,
             500,
             updateProcessError
        );
   }
}

// DELETE /api/members/:maimaiId (Authenticated User) - MODIFIED to use kindeUserId for auth
async function handleUserDeleteMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/members/:maimaiId DELETE request for Kinde user ID: ${kindeUserId}`);
    const parts = new URL(request.url).pathname.split('/');
    if (parts.length !== 4 || !parts[3]) {
        return errorResponse('Invalid API path. Use /api/members/:maimaiId', 400);
    }
    const targetMaimaiId = parts[3]; // The Maimai ID from the URL path

    // --- Authorize ---
    // Find the member by Maimai ID AND Kinde User ID
    const existingMember = await env.DB.prepare('SELECT id, team_code, avatar_url FROM members WHERE maimai_id = ? AND kinde_user_id = ?')
        .bind(targetMaimaiId, kindeUserId)
        .first<{ id: number, team_code: string, avatar_url?: string | null }>();

    if (!existingMember) {
        console.log(`User delete request for non-existent or unauthorized member: ${targetMaimaiId} (Kinde ID: ${kindeUserId})`);
        return errorResponse('Member not found or does not belong to your account.', 404);
    }

    // --- Execute Delete ---
    try {
        const teamCode = existingMember.team_code;
        const avatarUrlToDelete = existingMember.avatar_url;

        console.log(`Attempting to delete member record for ID ${existingMember.id} (Maimai ID: ${targetMaimaiId})`);
        const deleteResult = await env.DB.prepare('DELETE FROM members WHERE id = ?')
            .bind(existingMember.id)
            .run();

        if (!deleteResult.success) {
            console.error(`User delete member database operation failed for ID ${existingMember.id}:`, deleteResult.error);
            return errorResponse(deleteResult.error || 'Failed to delete member due to a database issue.', 500);
        }
        if (deleteResult.meta.changes === 0) {
             console.warn(`User delete query executed for ID ${existingMember.id} but no rows changed.`);
             return errorResponse('Member not found or already deleted.', 404);
        }
        console.log(`Successfully deleted member record for ID ${existingMember.id}.`);

        if (avatarUrlToDelete) {
             console.log(`Attempting to delete associated avatar: ${avatarUrlToDelete}`);
             ctx.waitUntil(deleteAvatarFromR2(env, avatarUrlToDelete));
        }

       ctx.waitUntil(checkAndDeleteEmptyTeam(env, teamCode));

       return new Response(null, { status: 204, headers: CORS_HEADERS });

    } catch (deleteProcessError) {
        console.error(`Error during user member deletion process for ID ${existingMember.id}:`, deleteProcessError);
        return errorResponse(
           `Failed to process deletion: ${deleteProcessError instanceof Error ? deleteProcessError.message : 'Unknown error'}`,
            500,
            deleteProcessError
        );
    }
}


// POST /api/member_song_preferences (Authenticated User) - MODIFIED to use kindeUserId
async function handleSaveMemberSongPreference(request: Request, env: Env, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/member_song_preferences POST for Kinde user ID: ${kindeUserId}`);
    try {
        const payload: SaveMemberSongPreferencePayload = await request.json();

        // 1. Find the member ID for the authenticated Kinde user
        const member = await env.DB.prepare('SELECT id FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ id: number }>();

        if (!member) {
            return errorResponse("Authenticated user is not registered as a member.", 403); // Forbidden
        }

        // 2. Validate payload and ensure member_id matches the authenticated user's member ID
        if (!payload.tournament_stage || !payload.song_id || !payload.selected_difficulty || payload.member_id !== member.id) {
             // Optionally allow admin to save for other members, but for user endpoint, enforce self-save
             return errorResponse("Invalid payload or member_id mismatch.", 400);
        }

        // 3. Check if song exists
        const songExists = await env.DB.prepare("SELECT id FROM songs WHERE id = ?").bind(payload.song_id).first();
        if (!songExists) {
             return errorResponse("Invalid song_id.", 400);
        }

        // 4. Insert/Update preference (using ON CONFLICT)
        const stmt = env.DB.prepare(
            `INSERT INTO member_song_preferences (member_id, tournament_stage, song_id, selected_difficulty, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(member_id, tournament_stage, song_id, selected_difficulty) DO UPDATE SET
                 selected_difficulty = excluded.selected_difficulty,
                 created_at = excluded.created_at
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
            // Fetch the inserted/updated preference with song details to return
            const newPreferenceQuery = `
                SELECT
                    msp.*,
                    s.title AS song_title,
                    s.cover_filename AS cover_filename,
                    s.levels_json AS levels_json
                FROM member_song_preferences msp
                JOIN songs s ON msp.song_id = s.id
                WHERE msp.member_id = ? AND msp.tournament_stage = ? AND msp.song_id = ? AND msp.selected_difficulty = ?`;
            const newPreference = await env.DB.prepare(newPreferenceQuery).bind(
                payload.member_id,
                payload.tournament_stage,
                payload.song_id,
                payload.selected_difficulty
            ).first<MemberSongPreference & { levels_json: string | null; song_title: string; cover_filename: string | null }>();

            if (newPreference) {
                 (newPreference as MemberSongPreference).parsedLevels = newPreference.levels_json ? JSON.parse(newPreference.levels_json) : undefined;
                 (newPreference as MemberSongPreference).fullCoverUrl = newPreference.cover_filename && env.R2_PUBLIC_BUCKET_URL
                   ? `${env.R2_PUBLIC_BUCKET_URL}/${newPreference.cover_filename}`
                   : undefined;
                 delete (newPreference as any).levels_json;
            }

            return jsonResponse(newPreference, 201);
        } else {
            console.error("Worker: Failed to save member song preference:", result.error);
            return errorResponse(result.error || "Failed to save preference.");
        }

    } catch (e: any) {
        console.error("Worker: Exception saving member song preference:", e);
        return errorResponse(e.message);
    }
}

// GET /api/member_song_preferences?member_id=:id&stage=:stage (Authenticated User) - MODIFIED to use kindeUserId
async function handleFetchMemberSongPreferences(request: Request, env: Env, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/member_song_preferences GET for Kinde user ID: ${kindeUserId}`);
    try {
        const url = new URL(request.url);
        const memberIdParam = url.searchParams.get('member_id');
        const stage = url.searchParams.get('stage');

        // 1. Find the member ID for the authenticated Kinde user
        const member = await env.DB.prepare('SELECT id FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ id: number }>();

        if (!member) {
            return errorResponse("Authenticated user is not registered as a member.", 403); // Forbidden
        }

        // 2. Validate parameters and ensure member_id matches the authenticated user's member ID
        const memberIdNum = memberIdParam ? parseInt(memberIdParam, 10) : null;

        if (!stage || memberIdNum === null || isNaN(memberIdNum) || memberIdNum !== member.id) {
             // Optionally allow admin to fetch for other members, but for user endpoint, enforce self-fetch
             return errorResponse("Invalid parameters or member_id mismatch.", 400);
        }

        // 3. Fetch preferences
        const query = `
            SELECT
                msp.*,
                s.title AS song_title,
                s.cover_filename AS cover_filename,
                s.levels_json AS levels_json
            FROM member_song_preferences msp
            JOIN songs s ON msp.song_id = s.id
            WHERE msp.member_id = ? AND msp.tournament_stage = ?
        `;

        const { results } = await env.DB.prepare(query).bind(memberIdNum, stage).all<MemberSongPreference & { levels_json: string | null; song_title: string; cover_filename: string | null }>();

        const preferencesWithDetails = results.map(pref => {
            const parsedLevels = pref.levels_json ? JSON.parse(pref.levels_json) as SongLevel : undefined;
            const fullCoverUrl = pref.cover_filename && env.R2_PUBLIC_BUCKET_URL
                ? `${env.R2_PUBLIC_BUCKET_URL}/${pref.cover_filename}`
                : undefined;
            const { levels_json, ...rest } = pref;
            return { ...rest, parsedLevels, fullCoverUrl };
        });

        return jsonResponse(preferencesWithDetails);

    } catch (e: any) {
        console.error("Worker: Failed to get member song preferences:", e);
        return errorResponse(e.message);
    }
}


// --- Admin API Endpoints (Require Admin Auth) ---

// GET /api/admin/members (Admin Only) - MODIFIED to use adminAuthMiddleware
async function handleAdminFetchMembers(request: Request, env: Env, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} fetching all members...`);
    // Admin authentication and isAdmin check already done by middleware

    try {
        // Fetch all members, including kinde_user_id and is_admin
        const allMembers = await env.DB.prepare(
            'SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members ORDER BY team_code ASC, joined_at ASC'
        ).all<Member>(); // Use Member type
        return jsonResponse({ members: allMembers.results || [] }, 200);
    } catch (e: any) {
        console.error('Database error fetching all members for admin:', e);
        return errorResponse('Failed to fetch all members from database.', 500, e);
    }
}

// TODO: Implement other admin handlers (handleAdminAddMember, handleAdminPatchMember, handleAdminDeleteMember, handleAdminUpdateSettings)
// These will also need to accept kindeUserId and be wrapped in adminAuthMiddleware.
// Example signature: async function handleAdminAddMember(request: Request, env: Env, kindeUserId: string): Promise<Response> { ... }


// --- Tournament/Match API Handlers ---
// Decide which of these require authentication (likely all except maybe GET list)
// fetchTournamentMatches (GET /api/tournament_matches) - Public (already handled)
// createTournamentMatch (POST /api/tournament_matches) - Requires Admin Auth
// confirmMatchSetup (PUT /api/tournament_matches/:id/confirm_setup) - Requires Admin Auth
// startLiveMatch (POST /api/tournament_matches/:id/start_live) - Requires Admin Auth
// fetchMatchState (GET /api/live-match/:doId/state) - Public (already handled)
// WebSocket (GET /api/live-match/:doId/websocket) - Public (already handled)
// calculateRound (POST /api/live-match/:doId/calculate-round) - Requires Admin Auth
// nextRound (POST /api/live-match/:doId/next-round) - Requires Admin Auth
// archiveMatch (POST /api/live-match/:doId/archive) - Requires Admin Auth
// resolveDraw (POST /api/live-match/:doId/resolve-draw) - Requires Admin Auth
// selectTiebreakerSong (POST /api/live-match/:doId/select-tiebreaker-song) - Requires Admin Auth
// fetchMatchHistory (GET /api/match_history) - Public (already handled)


// POST /api/tournament_matches (Admin Only) - MODIFIED to use adminAuthMiddleware
async function handleCreateTournamentMatch(request: Request, env: Env, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/tournament_matches POST request...`);
    try {
        interface SimpleCreateTournamentMatchPayload {
            round_name: string;
            team1_id: number | null;
            team2_id: number | null;
            scheduled_time?: string | null;
        }
        const payload: SimpleCreateTournamentMatchPayload = await request.json();

        if (!payload.round_name || payload.team1_id === null || payload.team2_id === null) {
             return errorResponse("Missing required fields: round_name, team1_id, team2_id", 400);
        } else {
            const team1 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team1_id).first();
            const team2 = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(payload.team2_id).first();
            if (!team1 || !team2) {
                 return errorResponse("Invalid team1_id or team2_id", 400);
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
                    'scheduled',
                    new Date().toISOString(),
                    new Date().toISOString()
                ).run();

                if (result.success) {
                    const newMatch = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(result.meta.last_row_id).first<TournamentMatch>();
                    return jsonResponse(newMatch, 201);
                } else {
                    console.error("Worker: Failed to create tournament match:", result.error);
                    return errorResponse(result.error || "Failed to create match.");
                }
            }
        }
    } catch (e: any) {
        console.error("Worker: Exception creating tournament match:", e);
        return errorResponse(e.message);
    }
}

// PUT /api/tournament_matches/:id/confirm_setup (Admin Only) - MODIFIED to use adminAuthMiddleware
async function handleConfirmMatchSetup(request: Request, env: Env, kindeUserId: string, tournamentMatchId: number): Promise<Response> {
     console.log(`Admin user ${kindeUserId} handling /api/tournament_matches/${tournamentMatchId}/confirm_setup PUT request...`);
     try {
         interface ConfirmSetupPayload {
             team1_player_order: number[];
             team2_player_order: number[];
             match_song_list: MatchSong[];
         }
         const payload: ConfirmSetupPayload = await request.json();

         if (!Array.isArray(payload.team1_player_order) || !Array.isArray(payload.team2_player_order) || !Array.isArray(payload.match_song_list) || payload.team1_player_order.length === 0 || payload.team2_player_order.length === 0 || payload.match_song_list.length === 0) {
             return errorResponse("Invalid payload: player orders and song list must be non-empty arrays.", 400);
         } else {
             const match = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<TournamentMatch>();
             if (!match) {
                 return errorResponse("Tournament match not found.", 404);
             } else if (match.status !== 'scheduled' && match.status !== 'pending_song_confirmation') {
                  return errorResponse(`Match status is '${match.status}'. Must be 'scheduled' or 'pending_song_confirmation' to confirm setup.`, 400);
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
                     'ready_to_start',
                     new Date().toISOString(),
                     tournamentMatchId
                 ).run();

                 if (result.success) {
                     const updatedMatch = await env.DB.prepare("SELECT * FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<TournamentMatch>();
                     return jsonResponse(updatedMatch);
                 } else {
                     console.error("Worker: Failed to confirm match setup:", result.error);
                     return errorResponse(result.error || "Failed to confirm setup.");
                 }
             }
         }
     } catch (e: any) {
         console.error(`Worker: Exception confirming match setup ${tournamentMatchId}:`, e);
         return errorResponse(e.message);
     }
}

// POST /api/tournament_matches/:id/start_live (Admin Only) - MODIFIED to use adminAuthMiddleware
async function handleStartLiveMatch(request: Request, env: Env, kindeUserId: string, tournamentMatchId: number): Promise<Response> {
     console.log(`Admin user ${kindeUserId} handling /api/tournament_matches/${tournamentMatchId}/start_live POST request...`);
     try {
         const match = await env.DB.prepare(
             `SELECT tm.*, t1.name AS team1_name, t2.name AS team2_name
              FROM tournament_matches tm
              JOIN teams t1 ON tm.team1_id = t1.id
              JOIN teams t2 ON tm.team2_id = t2.id
              WHERE tm.id = ?`
         ).bind(tournamentMatchId).first<TournamentMatch>();

         if (!match) {
             return errorResponse("Scheduled match not found", 404);
         } else if (match.status === 'live' && match.match_do_id) {
             console.log(`Worker: Match ${tournamentMatchId} is already live with DO ${match.match_do_id}. Returning existing DO ID.`);
             return jsonResponse({ message: "Match is already live.", match_do_id: match.match_do_id });
         } else if (match.status !== 'ready_to_start') {
              return errorResponse(`Match status is '${match.status}'. Must be 'ready_to_start' to start live.`, 400);
         } else if (!match.team1_player_order_json || !match.team2_player_order_json || !match.match_song_list_json) {
              return errorResponse("Match setup is incomplete (player order or song list missing).", 400);
         } else {
             const team1 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team1_id).first<{ code: string }>();
             const team2 = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(match.team2_id).first<{ code: string }>();
             if (!team1 || !team2) {
                  return errorResponse("Could not fetch team codes for members.", 500);
             } else {
                 const team1Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team1.code).all<Member>();
                 const team2Members = await env.DB.prepare("SELECT * FROM members WHERE team_code = ?").bind(team2.code).all<Member>();

                 if (!team1Members.results || team1Members.results.length === 0 || !team2Members.results || team2Members.results.length === 0) {
                       return errorResponse("One or both teams have no members assigned.", 400);
                  } else {
                     const team1PlayerOrderIds: number[] = JSON.parse(match.team1_player_order_json);
                     const team2PlayerOrderIds: number[] = JSON.parse(match.team2_player_order_json);
                     const matchSongList: MatchSong[] = JSON.parse(match.match_song_list_json);

                     if (!Array.isArray(team1PlayerOrderIds) || !Array.isArray(team2PlayerOrderIds) || !Array.isArray(matchSongList) || team1PlayerOrderIds.length === 0 || team2PlayerOrderIds.length === 0 || matchSongList.length === 0) {
                          return errorResponse("Parsed setup data is invalid.", 500);
                     } else {
                         const doIdString = `match-${tournamentMatchId}`;
                         const doStub = getMatchDO(doIdString, env);
                         const initPayload: MatchScheduleData = {
                             tournamentMatchId: tournamentMatchId,
                             round_name: match.round_name,
                             team1_id: match.team1_id,
                             team2_id: match.team2_id,
                             team1_name: match.team1_name || 'Team A',
                             team2_name: match.team2_name || 'Team B',
                             team1_members: team1Members.results,
                             team2_members: team2Members.results,
                             team1_player_order_ids: team1PlayerOrderIds,
                             team2_player_order_ids: team2PlayerOrderIds,
                             match_song_list: matchSongList,
                         };

                         // Forward initialization request to the DO
                         const doResponse = await forwardRequestToDO(doIdString, env, request, '/internal/initialize-from-schedule', 'POST', initPayload);

                         if (doResponse.ok) {
                             const updateStmt = env.DB.prepare("UPDATE tournament_matches SET status = ?, match_do_id = ?, updated_at = ? WHERE id = ?");
                             const updateResult = await updateStmt.bind('live', doIdString, new Date().toISOString(), tournamentMatchId).run();

                             if (updateResult.success) {
                                  console.log(`Worker: Tournament match ${tournamentMatchId} status updated to 'live' with DO ${doIdString}.`);
                                  const doResult = await doResponse.json();
                                  return jsonResponse({ message: "Match started live.", match_do_id: doIdString, do_init_result: doResult });
                             } else {
                                  console.error(`Worker: Failed to update tournament_matches status for ${tournamentMatchId}:`, updateResult.error);
                                  return errorResponse(`Match started live in DO, but failed to update schedule status: ${updateResult.error}`, 500);
                             }
                         } else {
                             const errorBody = await doResponse.json();
                             console.error(`Worker: Failed to initialize DO ${doIdString} for match ${tournamentMatchId}:`, errorBody);
                             return errorResponse(`Failed to initialize live match in Durable Object: ${errorBody.message || errorBody.error}`, doResponse.status);
                         }
                     }
                  }
             }
         }
     } catch (e: any) {
         console.error(`Worker: Exception starting live match ${tournamentMatchId}:`, e);
         return errorResponse(`Exception starting live match: ${e.message}`);
     }
}

// Live Match DO Actions (Admin Only) - MODIFIED to use adminAuthMiddleware
// These handlers read the body and then call forwardRequestToDO
async function handleAdminCalculateRound(request: Request, env: Env, kindeUserId: string, doIdString: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/live-match/${doIdString}/calculate-round POST request...`);
    try {
        const payload: CalculateRoundPayload = await request.json();
        if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
             return errorResponse("Invalid payload: teamA_percentage and teamB_percentage must be numbers.", 400);
        }
        // Forward the request to the DO. forwardRequestToDO will handle reading the body again.
        // A better approach is to read the body *once* here and pass it to forwardRequestToDO.
        // Let's modify forwardRequestToDO to accept an optional body parameter. (Already done above)
        return forwardRequestToDO(doIdString, env, request, '/internal/calculate-round', 'POST', payload);
    } catch (e: any) {
        console.error(`Worker: Exception processing calculate-round payload for DO ${doIdString}:`, e);
        return errorResponse(`Invalid payload format: ${e.message}`, 400);
    }
}

async function handleAdminNextRound(request: Request, env: Env, kindeUserId: string, doIdString: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/live-match/${doIdString}/next-round POST request...`);
    // Forward the request to the DO
    return forwardRequestToDO(doIdString, env, request, '/internal/next-round', 'POST');
}

async function handleAdminArchiveMatch(request: Request, env: Env, kindeUserId: string, doIdString: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/live-match/${doIdString}/archive POST request...`);
    // Forward the request to the DO
    return forwardRequestToDO(doIdString, env, request, '/internal/archive-match', 'POST');
}

async function handleAdminResolveDraw(request: Request, env: Env, kindeUserId: string, doIdString: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/live-match/${doIdString}/resolve-draw POST request...`);
    try {
        const payload: ResolveDrawPayload = await request.json();
        if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
             return errorResponse("Invalid payload: winner must be 'teamA' or 'teamB'.", 400);
         }
        // Forward the request to the DO
        return forwardRequestToDO(doIdString, env, request, '/internal/resolve-draw', 'POST', payload);
    } catch (e: any) {
        console.error(`Worker: Exception processing resolve-draw payload for DO ${doIdString}:`, e);
        return errorResponse(`Invalid payload format: ${e.message}`, 400);
    }
}

async function handleAdminSelectTiebreakerSong(request: Request, env: Env, kindeUserId: string, doIdString: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/live-match/${doIdString}/select-tiebreaker-song POST request...`);
    try {
        const payload: SelectTiebreakerSongPayload = await request.json();
        if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string') {
            return errorResponse("Invalid select-tiebreaker-song payload: song_id (number) and selected_difficulty (string) are required.", 400);
        }
        // Before forwarding, fetch song details from D1 to pass to DO
        const song = await env.DB.prepare("SELECT * FROM songs WHERE id = ?").bind(payload.song_id).first<Song>();
        if (!song) {
             return errorResponse(`Song with ID ${payload.song_id} not found in D1.`, 404);
         }
         // Pass song details along with selection to DO
        const doPayload = { song_id: payload.song_id, selected_difficulty: payload.selected_difficulty, song_details: song };
        // Forward the request to the DO
        return forwardRequestToDO(doIdString, env, request, '/internal/select-tiebreaker-song', 'POST', doPayload);
    } catch (e: any) {
        console.error(`Worker: Exception processing select-tiebreaker-song payload for DO ${doIdString}:`, e);
        return errorResponse(`Invalid payload format: ${e.message}`, 400);
    }
}


// --- Song Endpoints (Public) ---
// handleFetchSongs (GET /api/songs) (Keep as is)
async function handleFetchSongs(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/songs request...');
    try {
        const url = new URL(request.url);
        const page = parseInt(url.searchParams.get('page') || '1', 10);
        const limit = parseInt(url.searchParams.get('limit') || '20', 10);
        const offset = (page - 1) * limit;

        let baseQuery = "FROM songs WHERE 1=1";
        const params: (string | number)[] = [];
        const countParams: (string | number)[] = [];

        const category = url.searchParams.get('category');
        if (category) { baseQuery += " AND category = ?"; params.push(category); countParams.push(category); }
        const type = url.searchParams.get('type');
         if (type) { baseQuery += " AND type = ?"; params.push(type); countParams.push(type); }
         const search = url.searchParams.get('search');
         if (search) { baseQuery += " AND title LIKE ?"; params.push(`%${search}%`); countParams.push(`%${search}%`); }

         const countQuery = `SELECT COUNT(*) as total ${baseQuery}`;
         const dataQuery = `SELECT * ${baseQuery} ORDER BY title ASC LIMIT ? OFFSET ?`;
         params.push(limit, offset);

        const countStmt = env.DB.prepare(countQuery).bind(...countParams);
        const dataStmt = env.DB.prepare(dataQuery).bind(...params);

        const [{ total }, { results }] = await Promise.all([
            countStmt.first<{ total: number }>(),
            dataStmt.all<Song>()
        ]);

        const songsWithDetails = results.map((song) => {
            if (!song) return null;
            const parsedLevels = song.levels_json ? JSON.parse(song.levels_json) as SongLevel : undefined;
            const fullCoverUrl = song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                ? `${env.R2_PUBLIC_BUCKET_URL}/${song.cover_filename}`
                : undefined;
            return { ...song, parsedLevels, fullCoverUrl };
        }).filter(song => song !== null) as Song[]; // Cast back to Song[]

        const pagination: PaginationInfo = {
            currentPage: page,
            pageSize: limit,
            totalItems: total ?? 0,
            totalPages: Math.ceil((total ?? 0) / limit)
        };

        return jsonResponse<SongsApiResponseData>({ songs: songsWithDetails, pagination: pagination });

    } catch (e: any) {
        console.error("Worker: Failed to list songs:", e);
        if (e.cause) { console.error("Worker: D1 Error Cause:", e.cause); }
        return errorResponse(e.message);
    }
}

// handleFetchSongFilterOptions (GET /api/songs/filters) (Keep as is)
async function handleFetchSongFilterOptions(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/songs/filters request...');
    try {
        const categoryQuery = "SELECT DISTINCT category FROM songs WHERE category IS NOT NULL AND category != '' ORDER BY category";
        const typeQuery = "SELECT DISTINCT type FROM songs WHERE type IS NOT NULL AND type != '' ORDER BY type";

        const categoryStmt = env.DB.prepare(categoryQuery);
        const typeStmt = env.DB.prepare(typeQuery);

        const [{ results: categories }, { results: types }] = await Promise.all([
            categoryStmt.all<{ category: string }>(),
            typeStmt.all<{ type: string }>()
        ]);

        const categoryList = categories.map(c => c.category).filter(Boolean);
        const typeList = types.map(t => t.type).filter(Boolean);

        return jsonResponse<SongFiltersApiResponseData>({ categories: categoryList, types: typeList });

    } catch (e: any) {
        console.error("Worker: Failed to get song filter options:", e);
        return errorResponse(e.message);
    }
}


// handleFetchTournamentMatches (GET /api/tournament_matches) (Public) (Keep as is)
async function handleFetchTournamentMatches(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/tournament_matches request...');
    try {
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
        `;

        const { results } = await env.DB.prepare(query).all<TournamentMatch & { team1_player_order_json?: string | null; team2_player_order_json?: string | null; match_song_list_json?: string | null; }>();

        const matchesWithParsedData = results.map(match => ({
            ...match,
            team1_player_order: match.team1_player_order_json ? JSON.parse(match.team1_player_order_json) as number[] : null,
            team2_player_order: match.team2_player_order_json ? JSON.parse(match.team2_player_order_json) as number[] : null,
            match_song_list: match.match_song_list_json ? JSON.parse(match.match_song_list_json) as MatchSong[] : null,
        }));

        return jsonResponse(matchesWithParsedData);

    } catch (e: any) {
        console.error("Worker: Failed to list tournament matches:", e);
        if (e.cause) { console.error("Worker: D1 Error Cause:", e.cause); }
        return errorResponse(e.message);
    }
}

// handleFetchMatchHistory (GET /api/match_history) (Public) (Keep as is)
async function handleFetchMatchHistory(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/match_history request...');
    try {
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
        const { results } = await env.DB.prepare(matchesQuery).all<MatchHistoryMatch>(); // Use MatchHistoryMatch type

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
            const { results: rounds } = await env.DB.prepare(roundsQuery).bind(match.id).all<MatchHistoryRound & { round_summary_json: string | null; }>(); // Use MatchHistoryRound type

            const roundsWithParsedData = rounds.map(round => ({
                ...round,
                round_summary: round.round_summary_json ? JSON.parse(round.round_summary_json) as RoundSummary : null,
                fullCoverUrl: round.cover_filename && env.R2_PUBLIC_BUCKET_URL
                   ? `${env.R2_PUBLIC_BUCKET_URL}/${round.cover_filename}`
                   : undefined,
            }));

            return {
                ...match,
                rounds: roundsWithParsedData,
            };
        });

        const matchHistory = await Promise.all(historyPromises);

        return jsonResponse(matchHistory);

    } catch (e: any) {
        console.error("Worker: Failed to fetch match history:", e);
        return errorResponse(e.message);
    }
}


// --- Worker Entry Point (The Router) ---
export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);
        const method = request.method;
        const pathname = url.pathname;

        // Handle CORS preflight requests (Keep as is)
        if (method === 'OPTIONS') {
            return new Response(null, { headers: CORS_HEADERS });
        }

        let responseFromRoute: Response;

        // --- Public API Endpoints (No Auth Required) ---
        if (method === 'GET' && pathname === '/api/settings') {
            responseFromRoute = await handleGetSettings(request, env);
        } else if (method === 'POST' && pathname === '/api/kinde/callback') {
            responseFromRoute = await handleKindeCallback(request, env, ctx);
        } else if (method === 'GET' && pathname === '/api/logout') {
             responseFromRoute = await handleLogout(request, env);
        } else if (method === 'POST' && pathname === '/api/teams/check') {
            responseFromRoute = await handleCheckTeam(request, env);
        } else if (method === 'POST' && pathname === '/api/teams/create') {
            responseFromRoute = await handleCreateTeam(request, env);
        } else if (method === 'GET' && pathname.match(/^\/api\/teams\/[0-9]{4}$/)) {
             responseFromRoute = await handleGetTeamByCode(request, env);
        } else if (method === 'GET' && pathname === '/api/songs') {
             responseFromRoute = await handleFetchSongs(request, env);
        } else if (method === 'GET' && pathname === '/api/songs/filters') {
             responseFromRoute = await handleFetchSongFilterOptions(request, env);
        } else if (method === 'GET' && pathname === '/api/tournament_matches') {
             responseFromRoute = await handleFetchTournamentMatches(request, env);
        } else if (method === 'GET' && pathname === '/api/match_history') {
             responseFromRoute = await handleFetchMatchHistory(request, env);
        }
        // Live Match State and WebSocket might be public view
        else if (method === 'GET' && pathname.match(/^\/api\/live-match\/[^/]+\/state$/)) {
             const doIdString = pathname.split('/')[3];
             responseFromRoute = await forwardRequestToDO(doIdString, env, request, '/state', 'GET');
        } else if (method === 'GET' && pathname.match(/^\/api\/live-match\/[^/]+\/websocket$/)) {
             const doIdString = pathname.split('/')[3];
             const doStub = getMatchDO(doIdString, env);
             const doUrl = new URL(request.url);
             doUrl.pathname = '/websocket';
             // WebSocket forwarding needs direct response return, handled below
             responseFromRoute = await doStub.fetch(doUrl.toString(), request);
        }
        // Public GET for individual members (by ID)
        else if (method === 'GET' && pathname.match(/^\/api\/members\/\d+$/)) {
             responseFromRoute = await handleGetMemberById(request, env);
        }
        // Public GET for members list (can filter by team_code)
        else if (method === 'GET' && pathname === '/api/members') {
             responseFromRoute = await handleFetchMembers(request, env);
        }


        // --- Authenticated User API Endpoints (Require Kinde Auth) ---
        // Use authMiddleware to protect these routes
        else if (method === 'GET' && pathname === '/api/members/me') {
            responseFromRoute = await authMiddleware(request, env, ctx, handleFetchMe);
        } else if (method === 'POST' && pathname === '/api/teams/join') {
            responseFromRoute = await authMiddleware(request, env, ctx, handleJoinTeam);
        } else if (method === 'PATCH' && pathname.match(/^\/api\/members\/[^/]+$/)) { // Match /api/members/:maimaiId
             responseFromRoute = await authMiddleware(request, env, ctx, handleUserPatchMember);
        } else if (method === 'DELETE' && pathname.match(/^\/api\/members\/[^/]+$/)) { // Match /api/members/:maimaiId
             responseFromRoute = await authMiddleware(request, env, ctx, handleUserDeleteMember);
        } else if (method === 'POST' && pathname === '/api/member_song_preferences') {
             responseFromRoute = await authMiddleware(request, env, ctx, handleSaveMemberSongPreference);
        } else if (method === 'GET' && pathname === '/api/member_song_preferences') {
             responseFromRoute = await authMiddleware(request, env, ctx, handleFetchMemberSongPreferences);
        }


        // --- Admin API Endpoints (Require Admin Auth) ---
        // Use adminAuthMiddleware to protect these routes
        else if (method === 'GET' && pathname === '/api/admin/members') {
             responseFromRoute = await adminAuthMiddleware(request, env, ctx, handleAdminFetchMembers);
        }
        // Tournament/Match Admin Actions
        else if (method === 'POST' && pathname === '/api/tournament_matches') {
             responseFromRoute = await adminAuthMiddleware(request, env, ctx, handleCreateTournamentMatch);
        }
        else if (method === 'PUT' && pathname.match(/^\/api\/tournament_matches\/\d+\/confirm_setup$/)) {
             const tournamentMatchId = parseInt(pathname.split('/')[3], 10);
             if (!isNaN(tournamentMatchId)) {
                 responseFromRoute = await adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => handleConfirmMatchSetup(req, env, userId, tournamentMatchId));
             } else { responseFromRoute = errorResponse("Invalid tournament match ID", 400); }
        }
        else if (method === 'POST' && pathname.match(/^\/api\/tournament_matches\/\d+\/start_live$/)) {
             const tournamentMatchId = parseInt(pathname.split('/')[3], 10);
             if (!isNaN(tournamentMatchId)) {
                 responseFromRoute = await adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => handleStartLiveMatch(req, env, userId, tournamentMatchId));
             } else { responseFromRoute = errorResponse("Invalid tournament match ID", 400); }
        }
        // Live Match DO Actions (forwarded via Worker)
        else if (pathname.match(/^\/api\/live-match\/[^/]+\/(calculate-round|next-round|archive|resolve-draw|select-tiebreaker-song)$/)) {
             const parts = pathname.split('/');
             const doIdString = parts[3];
             const action = parts[4]; // e.g., "calculate-round"
             // Map the URL action to the internal DO path
             const internalPath = `/internal/${action}`; // Assumes internal paths match API paths with /internal/ prefix

             responseFromRoute = await adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => {
                 // Forward the request to the DO *after* admin auth
                 // forwardRequestToDO will handle reading the body if needed
                 return forwardRequestToDO(doIdString, env, req, internalPath, req.method);
             });
        }


        // --- Fallback for unmatched routes ---
        else {
            responseFromRoute = new Response('Not Found.', { status: 404, headers: CORS_HEADERS }); // Add CORS headers to 404
        }

        // --- Add CORS headers *UNLESS* it's a WebSocket upgrade response ---
        // (Keep this logic as is)
        if (responseFromRoute.webSocket) {
            console.log("Worker: Returning WebSocket upgrade response directly.");
            return responseFromRoute;
        }

        // If it's NOT a WebSocket upgrade, ensure CORS headers are present
        // The jsonResponse and errorResponse helpers already add CORS_HEADERS,
        // but this ensures it for any other response types (like the 404 above).
        // We create a new Response to ensure headers are mutable if needed,
        // although jsonResponse/errorResponse already return new Responses.
        // This step might be redundant if all paths use jsonResponse/errorResponse or the WebSocket logic.
        // Let's keep it simple and assume jsonResponse/errorResponse handle headers.
        // The 404 case above was updated to add headers.

        return responseFromRoute; // Return the response generated by the route logic (which includes CORS headers)
    },
};

// Make the Durable Object class available (Keep as is)
export { MatchDO };

// TODO: Implement other Admin Handlers (handleAdminAddMember, handleAdminPatchMember, handleAdminDeleteMember, handleAdminUpdateSettings)
// These will need to accept kindeUserId from the middleware and perform actions based on admin privileges.
// Example: handleAdminPatchMember might update any member's record, not just the authenticated user's.
// They should also use env.R2_PUBLIC_BUCKET_URL for avatar URLs.
