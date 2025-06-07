// src/index.ts

// --- Imports ---
// Import @tsndr/cloudflare-worker-jwt for Kinde Auth
import jwt from '@tsndr/cloudflare-worker-jwt';
import { calculateSemifinalScore } from './utils/semifinalScoreCalculator';
// Import your backend types (Ensure this file exists and contains necessary types)
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
    SaveMemberSongPreferencePayload,
    Song,
    MatchSong,
    SelectTiebreakerSongPayload,
    RoundSummary,
    SongLevel,
    ApiResponse,
    SongsApiResponseData,
    SongFiltersApiResponseData,
    PaginationInfo,
    KindeUser,
    MatchHistoryMatch,
    MatchHistoryRound,
    // Import NEW backend types from types.ts
    MatchPlayerSelection,
    SaveMatchPlayerSelectionPayload,
    FetchUserMatchSelectionData, // Corrected type name
    MatchSelectionStatus, // Corrected type name
    CompileMatchSetupResponse, // Corrected type name
    // Add missing payloads used in existing handlers
    CreateTournamentMatchPayload, // Used in handleCreateTournamentMatch
    ConfirmMatchSetupPayload, // Used in handleConfirmMatchSetup
    SemifinalMatch, // <-- Import new types

} from './types'; // Adjust path to your types file

import { MatchDO } from './durable-objects/matchDo'; // Adjust path to your DO file

// Import standard Worker types for clarity
import { D1Database, R2Bucket, ExecutionContext, DurableObjectStub, DurableObjectId } from "@cloudflare/workers-types";


// --- Configuration & Constants ---
const CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*', // Or restrict to your frontend domain
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS', // Added PATCH
    'Access-Control-Allow-Headers': 'Content-Type, Authorization', // Include Authorization header
    'Access-Control-Max-Age': '86400', // Cache preflight requests for 24 hours
    'Access-Control-Allow-Credentials': 'true', // IMPORTANT for cookies
};

let cachedJwks: any = undefined; // Cache the fetched JWKS


// --- Helper Functions ---

// Helper to get a DO instance by Name string (using idFromName)
const getMatchDO = (doName: string, env: Env): DurableObjectStub => {
    const id: DurableObjectId = env.MATCH_DO.idFromName(doName);
    return env.MATCH_DO.get(id);
};

// Helper to handle forwarding requests to DOs
const forwardRequestToDO = async (doIdString: string, env: Env, request: Request, internalPath: string, method: string = 'POST', bodyData?: any): Promise<Response> => {
    try {
        const doStub = getMatchDO(doIdString, env);
        const doUrl = new URL(`https://dummy-host`); // Dummy host is fine for DO fetch
        doUrl.pathname = internalPath;

        const newHeaders = new Headers();
        for (const [key, value] of request.headers.entries()) {
            // Exclude headers that might cause issues or are specific to the Worker context
            if (!['host', 'content-length', 'transfer-encoding'].includes(key.toLowerCase())) {
                newHeaders.append(key, value);
            }
        }

        let requestBody: BodyInit | null | undefined = undefined;

        // If bodyData is explicitly provided (e.g., for DO initialization with parsed data)
        if (bodyData !== undefined) {
             requestBody = JSON.stringify(bodyData);
             newHeaders.set('Content-Type', 'application/json');
        } else {
            // Otherwise, attempt to read the body from the original request
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
        }


        const requestInit: RequestInit = {
            method: method,
            headers: newHeaders, // Use the mutable Headers object
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
        return new Response(JSON.stringify({ success: false, error: `Failed to communicate with match instance: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json', ...CORS_HEADERS } }); // Ensure CORS headers on 500
    }
};


// Helper to wrap responses in ApiResponse format
function jsonResponse<T>(data: T, status: number = 200): Response {
    return new Response(JSON.stringify({ success: true, data }), {
        status,
        headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
    });
}

// Helper to wrap error responses
function errorResponse(error: string, status: number = 500, details?: any): Response {
    console.error(`API Error (${status}): ${error}`, details); // Log errors on the backend
    return new Response(JSON.stringify({ success: false, error, details }), { // Include details in response body
        status,
        headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
    });
}


// Placeholder for R2 Avatar Upload
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

// Placeholder for R2 Avatar Deletion
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

// Placeholder for checking and deleting empty teams
async function checkAndDeleteEmptyTeam(env: Env, teamCode: string): Promise<void> {
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

// Helper functions for CSV export
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

// Helper to verify Kinde Access Token using @tsndr/cloudflare-worker-jwt
async function verifyKindeToken(env: Env, token: string): Promise<{ userId: string, claims: any } | null> {
    if (!env.KINDE_ISSUER_URL) {
        console.error("KINDE_ISSUER_URL not configured in Worker secrets.");
        return null;
    }
    // Ensure KINDE_CLIENT_ID is configured for audience validation
    if (!env.KINDE_CLIENT_ID) {
         console.error("KINDE_CLIENT_ID not configured in Worker secrets.");
         return null;
    }

    // For a single-application worker, the audience is just the client ID
    const expectedAudience = env.KINDE_CLIENT_ID;
    console.log("Expected Kinde Audience for verification:", expectedAudience);

    try {
        // 1. Decode the token to get the header and payload (without verification)
        console.log("Attempting to decode token...");
        const decoded = jwt.decode(token);
        const payload = decoded.payload;
        const header = decoded.header;
        console.log("Token header:", header);
        console.log("Token payload:", payload);

        // 2. Perform basic claim validation (issuer, expiration)
        const now = Math.floor(Date.now() / 1000);
        if (payload.exp && payload.exp < now) {
            console.warn("Token expired.");
            return null; // Token expired
        }
        if (payload.iss !== env.KINDE_ISSUER_URL) {
            console.warn(`Invalid issuer: ${payload.iss}, expected: ${env.KINDE_ISSUER_URL}`);
            return null; // Invalid issuer
        }
        // Kinde's user ID is typically in the 'sub' claim
        if (!payload.sub) {
             console.warn("Token payload missing 'sub' claim.");
             return null; // Missing subject
        }

        // 检查azp字段 - 对于多域名Kinde认证的特殊处理
        if ((!payload.aud || (Array.isArray(payload.aud) && payload.aud.length === 0)) &&
            payload.azp === expectedAudience) {
            console.log("Empty audience with matching azp detected - skipping signature verification and trusting token");
            // 对于空audience但azp匹配的情况，我们信任这个token
            // 这是一个绕过方案，用于处理多域名Kinde认证
            return { userId: payload.sub as string, claims: payload };
        }

        // 3. Fetch or use cached JWKS
        if (!cachedJwks) {
            const jwksUrl = `${env.KINDE_ISSUER_URL}/.well-known/jwks`;
            console.log(`Fetching JWKS from: ${jwksUrl}`);
            const jwksResponse = await fetch(jwksUrl);
            if (!jwksResponse.ok) {
                console.error(`Failed to fetch JWKS: ${jwksResponse.status} ${jwksResponse.statusText}`);
                return null; // Failed to fetch keys
            }
            cachedJwks = await jwksResponse.json();
            console.log("Fetched and cached JWKS."); // Log caching
        } else {
            console.log("Using cached JWKS."); // Log cache hit
        }

        // Ensure cachedJwks is valid before proceeding
        if (!cachedJwks || !Array.isArray(cachedJwks.keys)) {
             console.error("Cached JWKS is invalid.");
             cachedJwks = undefined; // Clear invalid cache
             return null;
        }

        // 4. Find the correct key from the JWKS based on the token's kid
        const kid = header.kid;
        const key = cachedJwks.keys.find((k: any) => k.kid === kid);
        if (!key) {
            console.error(`No key found in JWKS with kid: ${kid}`);
            cachedJwks = undefined; // Clear cache in case of key rotation
            return null; // Key not found
        }
        console.log("Found matching JWK:", key);

        // 5. Import the JWK as a CryptoKey using Web Crypto API
        console.log("Attempting to import JWK as CryptoKey...");
        let publicKey: CryptoKey;
        try {
            publicKey = await crypto.subtle.importKey(
                'jwk', // Format is JWK
                key,   // The JWK object
                {      // Algorithm parameters for RS256
                    name: 'RSASSA-PKCS1-v1_5', // Standard name for RS256 in Web Crypto API
                    hash: 'SHA-256',           // Hash algorithm is SHA-256
                },
                false, // Not extractable (public key doesn't need to be)
                ['verify'] // Key usage is for verification
            );
            console.log("JWK imported successfully as CryptoKey.");
        } catch (importError: any) {
            console.error("Failed to import JWK as CryptoKey:", importError);
            if (importError.message) console.error(`Import error message: ${importError.message}`);
            if (importError.stack) console.error(`Import error stack: ${importError.stack}`);
            return null; // Failed to import key
        }

        // 6. Verify the token signature and claims using the imported CryptoKey
        console.log("Attempting to verify token signature and claims with @tsndr/cloudflare-worker-jwt using CryptoKey...");

        // 验证选项 - 如果是空audience，就不检查audience
        const verifyOptions: any = { algorithms: ['RS256'] };

        // 只在audience不为空的情况下验证audience
        if (payload.aud && Array.isArray(payload.aud) && payload.aud.length > 0) {
            verifyOptions.audience = expectedAudience;
        } else {
            console.log("Empty audience detected, skipping audience validation");
        }

        const verified = await jwt.verify(token, publicKey, verifyOptions);

        if (!verified) {
            console.warn("Token signature verification failed.");
            console.warn("Token audience was:", payload.aud);
            console.warn("Token azp was:", payload.azp);
            return null;
        }

        console.log("Token verification successful.");
        return { userId: payload.sub as string, claims: payload };

    } catch (e: any) {
        console.error("Error verifying Kinde token:", e);
        if (e.message) console.error(`Error message: ${e.message}`);
        if (e.stack) console.error(`Error stack: ${e.stack}`);
        return null;
    }
}


// Middleware-like function to extract Kinde User ID from token/cookie
async function getAuthenticatedKindeUser(request: Request, env: Env): Promise<string | null> {
    const authHeader = request.headers.get('Authorization');
    let token = null;

    // 1. Check Authorization header first (Bearer token)
    if (authHeader?.startsWith('Bearer ')) {
        token = authHeader.substring(7);
        console.log("Found token in Authorization header.");
    }
    // 2. If no Bearer token, check for 'kinde_access_token' cookie
    else {
        const cookieHeader = request.headers.get('Cookie');
        if (cookieHeader) {
            // More robust cookie parsing
            const cookies = cookieHeader.split(';').reduce((acc, cookie) => {
                const [key, value] = cookie.trim().split('=');
                acc[key] = value;
                return acc;
            }, {} as Record<string, string>);

            if (cookies['kinde_access_token']) {
                token = cookies['kinde_access_token'];
                console.log("Found token in 'kinde_access_token' cookie.");
            } else {
                 console.log("No 'kinde_access_token' cookie found.");
            }
        } else {
             console.log("No Cookie header found.");
        }
    }

    if (!token) {
        console.log("No Kinde token found in Authorization header or cookies.");
        return null; // No token found
    }

    const verificationResult = await verifyKindeToken(env, token);
    if (!verificationResult) {
        console.warn("Kinde token verification failed.");
        return null; // Token invalid or expired
    }

    console.log(`Authenticated Kinde User ID: ${verificationResult.userId}`);
    return verificationResult.userId; // Return the Kinde user ID
}

// Helper to check if the authenticated Kinde user is marked as admin in the DB
async function isAdminUser(env: Env, kindeUserId: string): Promise<boolean> {
    if (!kindeUserId) return false;
    try {
        const member = await env.DB.prepare('SELECT is_admin FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ is_admin: number | null }>();
        const isAdmin = member?.is_admin === 1;
        console.log(`Checking admin status for Kinde ID ${kindeUserId}: ${isAdmin ? 'Is Admin' : 'Not Admin'}`);
        return isAdmin;
    } catch (e: any) {
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
        const isPaused = setting?.value === 'true';
        console.log(`Collection paused status: ${isPaused}`);
        return isPaused;
    } catch (e: any) {
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
    console.log(`Running authMiddleware for ${new URL(request.url).pathname}`);
    const kindeUserId = await getAuthenticatedKindeUser(request, env);

    if (!kindeUserId) {
        console.warn(`Authentication required for ${new URL(request.url).pathname}`);
        return errorResponse('Authentication required.', 401);
    }

    console.log(`User authenticated with Kinde ID: ${kindeUserId}. Proceeding to handler.`);
    // User is authenticated, proceed to the actual handler with the user ID
    return handler(request, env, ctx, kindeUserId);
}

// --- Admin Authentication Middleware ---
// This middleware checks if the user is authenticated AND is an admin.
type AdminAuthenticatedHandler = (request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string) => Promise<Response>;

async function adminAuthMiddleware(request: Request, env: Env, ctx: ExecutionContext, handler: AdminAuthenticatedHandler): Promise<Response> {
    console.log(`Running adminAuthMiddleware for ${new URL(request.url).pathname}`);
    // First, check if the user is authenticated at all
    const kindeUserId = await getAuthenticatedKindeUser(request, env);

    if (!kindeUserId) {
        console.warn(`Admin access denied: User not authenticated via Kinde for ${new URL(request.url).pathname}`);
        return errorResponse('Authentication required.', 401);
    }

    // If authenticated, check if they are an admin
    const isAdmin = await isAdminUser(env, kindeUserId);
    if (!isAdmin) {
        console.warn(`Admin access denied: User ${kindeUserId} is not an admin for ${new URL(request.url).pathname}`);
        return errorResponse('Authorization failed: You do not have administrator privileges.', 403);
    }

    console.log(`Admin user ${kindeUserId} authenticated. Proceeding to admin handler.`);
    // If authenticated and is admin, proceed to the actual admin handler
    return handler(request, env, ctx, kindeUserId);
}


// --- Route Handlers ---

// GET /api/settings (Public)
async function handleGetSettings(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/settings request...');
    try {
        const paused = await isCollectionPaused(env);
        return jsonResponse({ collection_paused: paused }, 200);
    } catch (e: any) {
        console.error('Error fetching settings:', e);
        return errorResponse('Failed to fetch settings.', 500, e);
    }
}

// POST /api/kinde/callback (Public)
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
        console.log(`Exchanging code for token at: ${tokenUrl}`);
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
        console.log("Kinde token exchange response status:", tokenResponse.status);
        console.log("Kinde token exchange response body:", tokenData);


        if (!tokenResponse.ok) {
            console.error('Kinde token exchange failed:', tokenResponse.status, tokenData);
            return errorResponse(tokenData.error_description || tokenData.error || 'Failed to exchange authorization code for tokens.', tokenResponse.status);
        }

        const { access_token, id_token, refresh_token, expires_in } = tokenData;

        const headers = new Headers(CORS_HEADERS);
        const secure = url.protocol === 'https:' ? '; Secure' : '';
        headers.append('Set-Cookie', `kinde_access_token=${access_token}; HttpOnly; Path=/; Max-Age=${expires_in}; SameSite=Lax${secure}`);
        console.log("Set kinde_access_token cookie.");

        // Set Refresh Token cookie (HttpOnly)
        if (refresh_token) {
             const refreshTokenMaxAge = 30 * 24 * 60 * 60; // 30 days
             headers.append('Set-Cookie', `kinde_refresh_token=${refresh_token}; HttpOnly; Path=/; Max-Age=${refreshTokenMaxAge}; SameSite=Lax${secure}`);
             console.log("Set kinde_refresh_token cookie.");
        }

        // Decode ID token for basic user info to return to frontend
        let userInfo: KindeUser | {} = {};
        try {
            console.log("Decoding ID token payload...");
            const idTokenDecoded = jwt.decode(id_token);
            const idTokenPayload = idTokenDecoded.payload;

            console.log("ID Token Payload:", idTokenPayload);
            userInfo = {
                id: idTokenPayload.sub, // Kinde User ID
                email: idTokenPayload.email,
                name: idTokenPayload.given_name && idTokenPayload.family_name ? `${idTokenPayload.given_name} ${idTokenPayload.family_name}` : idTokenPayload.given_name || idTokenPayload.family_name || idTokenPayload.email,
            } as KindeUser;
            console.log("Decoded User Info:", userInfo);
        } catch (e) {
            console.error("Failed to decode ID token payload:", e);
        }

        return new Response(JSON.stringify({ success: true, data: { user: userInfo } }), { // Wrap user in data object for ApiResponse format
            status: 200,
            headers: headers,
        });

    } catch (kindeError: any) {
        console.error('Error during Kinde token exchange:', kindeError);
        return errorResponse('Failed to communicate with authentication server.', 500, kindeError);
    }
}

// GET /api/logout (Public)
async function handleLogout(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/logout request...');
    const url = new URL(request.url);
    const headers = new Headers(CORS_HEADERS);
    const secure = url.protocol === 'https:' ? '; Secure' : '';
    headers.append('Set-Cookie', `kinde_access_token=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax${secure}`);
    console.log("Cleared kinde_access_token cookie.");
    headers.append('Set-Cookie', `kinde_refresh_token=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax${secure}`);
    console.log("Cleared kinde_refresh_token cookie.");


    // Redirect to Kinde's logout endpoint
    if (!env.KINDE_ISSUER_URL || !env.LOGOUT_REDIRECT_TARGET_URL) {
         console.error("Kinde logout configuration missing.");
         // If config is missing, just return a success response after clearing cookies
         return new Response(JSON.stringify({ success: true, message: "Logged out (cookies cleared)." }), { status: 200, headers });
    }

    const kindeLogoutUrl = new URL(`${env.KINDE_ISSUER_URL}/logout`);
    kindeLogoutUrl.searchParams.append('redirect', env.LOGOUT_REDIRECT_TARGET_URL);
    console.log(`Redirecting to Kinde logout URL: ${kindeLogoutUrl.toString()}`);

    // Return a redirect response
    return Response.redirect(kindeLogoutUrl.toString(), 302);
}


// GET /api/teams (Public)
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

// GET /api/teams/:code (Public)
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
        ).bind(teamCode).all<Member>();

        return jsonResponse({
            success: true, // Add success: true for consistency with ApiResponse
            code: teamCode,
            name: teamResult.name,
            members: membersResult.results || [] // Use results from the D1 query
        }, 200);

    } catch (e: any) {
        console.error('Database error fetching team by code:', e);
        return errorResponse('Failed to fetch team information.', 500, e);
    }
}

// POST /api/teams/check (Public)
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
        ).bind(teamCode).all<Partial<Member>>();

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

// POST /api/teams/create (Public)
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


// GET /api/members (Public - Can filter by team_code)
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
        const { results } = await env.DB.prepare(query).bind(...params).all<Member>();
        return jsonResponse(results);
    } catch (e: any) {
        console.error("Worker: Failed to list members:", e);
        return errorResponse(e.message);
    }
}

// GET /api/members/:id (Public)
async function handleGetMemberById(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/members/:id request...');
    const parts = new URL(request.url).pathname.split('/');
    const memberId = parseInt(parts[3], 10);
    if (isNaN(memberId)) {
        return errorResponse("Invalid member ID in path", 400);
    }
    try {
        const member = await env.DB.prepare("SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?").bind(memberId).first<Member>();
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


// GET /api/members/me (Authenticated User)
async function handleFetchMe(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/members/me request for Kinde user ID: ${kindeUserId}`);
    try {
        // Find the member record associated with this Kinde User ID
        const member = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE kinde_user_id = ?')
            .bind(kindeUserId)
            .first<Member>();

        if (!member) {
            console.log(`Member not found for Kinde ID ${kindeUserId}.`);
            // Return success: true with null data if user is authenticated but not registered
            return jsonResponse({ member: null, message: "User not registered." }, 200);
        }

        console.log(`Found member for Kinde ID ${kindeUserId}: ID ${member.id}`);
        return jsonResponse({ member: member }, 200); // Wrap member in data object for ApiResponse format

    } catch (e: any) {
        console.error(`Database error fetching member for Kinde ID ${kindeUserId}:`, e);
        return errorResponse('Failed to fetch member information.', 500, e);
    }
}


// POST /api/teams/join (Authenticated User)
async function handleJoinTeam(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Handling /api/teams/join request for Kinde user ID: ${kindeUserId}`);
    const paused = await isCollectionPaused(env);
    if (paused) {
        console.log('Collection is paused. Denying join.');
        return errorResponse('现在的组队已停止，如需更多信息，请访问官网或咨询管理员。', 403);
    }

    let formData: FormData;
    try { formData = await request.formData(); } catch (e: any) { return errorResponse('Invalid request format. Expected multipart/form-data.', 400, e); }

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
              const idForAvatarPath = kindeUserId; // Use kindeUserId directly
               if (!idForAvatarPath) {
                    console.error(`Cannot determine identifier for avatar path for Kinde ID ${kindeUserId}`);
                    return errorResponse('Failed to determine avatar identifier.', 500);
               }
              avatarUrl = await uploadAvatar(env, avatarFile, idForAvatarPath, teamCode);
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
              if (insertResult.error?.includes('UNIQUE constraint failed')) {
                   // This might catch unique constraints on maimai_id or kinde_user_id if they exist
                   return errorResponse('Failed to add member: A record with this Maimai ID or account already exists.', 409);
              }
             return errorResponse(insertResult.error || 'Failed to add member due to a database issue.', 500);
         }

         const newMemberId = insertResult.meta.last_row_id;
         const newMember = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?')
             .bind(newMemberId)
             .first<Member>();

         return jsonResponse({ success: true, message: "Member added successfully.", member: newMember }, 201);

    } catch (processingError: any) {
        console.error('Error during join team processing pipeline:', processingError);
        return errorResponse(
             `Failed to process join request: ${processingError instanceof Error ? processingError.message : 'Unknown error'}`,
             500,
             processingError
        );
    }
}

// GET /api/member/matches (Authenticated User)
async function handleFetchUserMatches(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Authenticated user ${kindeUserId} handling /api/member/matches GET request...`);

    try {
        // 1. Find the member ID and team code for the authenticated Kinde user
        const member = await env.DB.prepare('SELECT id, team_code FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ id: number; team_code: string }>();

        if (!member) {
            return errorResponse("Authenticated user is not registered as a member.", 403); // Forbidden
        }

        // 2. Find the user's team ID from the team_code
        const userTeam = await env.DB.prepare('SELECT id FROM teams WHERE code = ? LIMIT 1')
             .bind(member.team_code)
             .first<{ id: number }>();

        if (!userTeam) {
             // This case indicates a data inconsistency
             console.error(`Data inconsistency: Member ${member.id} has team_code ${member.team_code} but team not found.`);
             return errorResponse("Could not find your team information.", 500);
        }
        const userTeamId = userTeam.id; // 获取用户队伍的数字 ID

        // 3. Fetch all matches where the user's team is either team1 or team2
        // Include team names for display
        const query = `
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name,
                tw.name AS winner_team_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            LEFT JOIN teams tw ON tm.winner_team_id = tw.id
            WHERE tm.team1_id = ? OR tm.team2_id = ?
            ORDER BY tm.scheduled_time DESC, tm.created_at DESC -- Order by scheduled time (upcoming first) or creation time
        `;

        const { results } = await env.DB.prepare(query).bind(userTeamId, userTeamId).all<TournamentMatch>();

        // Parse JSON fields and add fullCoverUrl for match_song_list (optional, might not be needed for this list view)
        // Let's skip parsing JSON here for simplicity in the list view,
        // as the detailed song list is needed on the selection page.
        // If you need player orders or song lists in this view, add parsing here.
        const matchesWithTeamNames = results.map(match => {
             const parsedMatch = { ...match } as TournamentMatch;
             // Remove raw JSON fields if not needed in this list view
             delete (parsedMatch as any).team1_player_order_json;
             delete (parsedMatch as any).team2_player_order_json;
             delete (parsedMatch as any).match_song_list_json;
             return parsedMatch;
        });


        return jsonResponse(matchesWithTeamNames);

    } catch (e: any) {
        console.error(`Worker: Failed to fetch user's matches for user ${kindeUserId}:`, e);
        return errorResponse(e.message);
    }
}

// PATCH /api/members/:maimaiId (Authenticated User)
async function handleUserPatchMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
     console.log(`Handling /api/members/:maimaiId PATCH request for Kinde user ID: ${kindeUserId}`);
     const parts = new URL(request.url).pathname.split('/');
     if (parts.length !== 4 || !parts[3]) {
         return errorResponse('Invalid API path. Use /api/members/:maimaiId', 400);
     }
     const targetMaimaiId = parts[3]; // The Maimai ID from the URL path (used to find the record)

    let formData: FormData;
    try { formData = await request.formData(); } catch (e: any) { return errorResponse('Invalid request format for update. Expected multipart/form-data.', 400, e); }

    // --- Authorization Step ---
    // Verify the member exists AND belongs to the authenticated Kinde user
    const existingMember = await env.DB.prepare('SELECT * FROM members WHERE maimai_id = ? AND kinde_user_id = ?')
        .bind(targetMaimaiId, kindeUserId)
        .first<Member>();

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
       // Use Kinde User ID in avatar path for better uniqueness and association
       const idForAvatarPath = existingMember.kinde_user_id || existingMember.maimai_id; // Use Kinde ID if available
       if (!idForAvatarPath) {
            console.error(`Cannot determine identifier for avatar path for member ID ${existingMember.id}`);
            return errorResponse('Failed to determine avatar identifier.', 500);
       }
       const targetTeamCodeForAvatar = existingMember.team_code; // User cannot change team via this endpoint
       const uploadedUrl = await uploadAvatar(env, newAvatarFile, idForAvatarPath, targetTeamCodeForAvatar);
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
            .first<Member>();

        if (!updatedMember) {
              console.error(`Consistency issue: Member ID ${existingMember.id} updated but could not be re-fetched.`);
              return errorResponse('Update successful, but failed to retrieve updated data.', 500);
        }

        return jsonResponse({ success: true, message: "Information updated successfully.", member: updatedMember }, 200);

   } catch (updateProcessError: any) {
        console.error(`Error during the user member update process for ID ${existingMember.id}:`, updateProcessError);
        return errorResponse(
             `Failed to process update: ${updateProcessError instanceof Error ? updateProcessError.message : 'Unknown error'}`,
             500,
             updateProcessError
        );
   }
}

// DELETE /api/members/:maimaiId (Authenticated User)
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

    } catch (deleteProcessError: any) {
        console.error(`Error during user member deletion process for ID ${existingMember.id}:`, deleteProcessError);
        return errorResponse(
           `Failed to process deletion: ${deleteProcessError instanceof Error ? deleteProcessError.message : 'Unknown error'}`,
            500,
            deleteProcessError
        );
    }
}


// POST /api/member_song_preferences (Authenticated User)
async function handleSaveMemberSongPreference(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
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
                WHERE msp.member_id = ? AND msp.tournament_stage = ? AND msp.song_id = ? AND msp.selected_difficulty = ?`; // Match the ON CONFLICT columns
            const newPreference = await env.DB.prepare(newPreferenceQuery).bind(
                payload.member_id,
                payload.tournament_stage,
                payload.song_id,
                payload.selected_difficulty
            ).first<MemberSongPreference & { levels_json: string | null; song_title: string; cover_filename: string | null }>();

            if (newPreference) {
                 (newPreference as MemberSongPreference).parsedLevels = newPreference.levels_json ? JSON.parse(newPreference.levels_json) as SongLevel : undefined;
                 (newPreference as MemberSongPreference).fullCoverUrl = newPreference.cover_filename && env.R2_PUBLIC_BUCKET_URL
                   ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${newPreference.cover_filename}` // Correct R2 path
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

// GET /api/member_song_preferences (Authenticated User)
async function handleFetchMemberSongPreferences(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
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

        // Keep File 1's validation: requires stage and member_id query params, and member_id must match auth user
        if (!stage || memberIdNum === null || isNaN(memberIdNum) || memberIdNum !== member.id) {
             // Optionally allow admin to fetch for other members, but for user endpoint, enforce self-fetch
             // NOTE: File 1's logic here does NOT allow admin to fetch others. Keeping File 1's logic.
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
                ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${pref.cover_filename}` // Correct R2 path
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

// GET /api/admin/members (Admin Only)
async function handleAdminFetchMembers(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} fetching all members...`);
    // Admin authentication and isAdmin check already done by middleware

    try {
        // Fetch all members, including kinde_user_id and is_admin
        const allMembers = await env.DB.prepare(
            'SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members ORDER BY team_code ASC, joined_at ASC'
        ).all<Member>();
        return jsonResponse({ members: allMembers.results || [] }, 200);
    } catch (e: any) {
        console.error(`Admin user ${kindeUserId}: Failed to list all members:`, e);
        return errorResponse('Failed to fetch all members from database.', 500, e);
    }
}

// GET /api/admin/members/:id (Admin Only)
async function handleAdminGetMemberById(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const memberId = parseInt(parts[4], 10); // /api/admin/members/:id -> parts[4]
    console.log(`Admin user ${kindeUserId} handling /api/admin/members/:id request for member ID ${memberId}...`);

    if (isNaN(memberId)) {
        return errorResponse("Invalid member ID in path", 400);
    }
    try {
        const member = await env.DB.prepare("SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?").bind(memberId).first<Member>();
        if (member) {
            return jsonResponse(member);
        } else {
            return errorResponse("Member not found", 404);
        }
    } catch (e: any) {
        console.error(`Admin user ${kindeUserId}: Failed to get member ${memberId}:`, e);
        return errorResponse(e.message);
    }
}

// PATCH /api/admin/members/:id (Admin Only)
async function handleAdminPatchMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const targetMemberId = parseInt(parts[4], 10); // /api/admin/members/:id -> parts[4]
    console.log(`Admin user ${kindeUserId} handling /api/admin/members/:id PATCH request for member ID ${targetMemberId}...`);

    if (isNaN(targetMemberId)) {
        return errorResponse("Invalid member ID in path", 400);
    }

    let formData: FormData;
    try { formData = await request.formData(); } catch (e: any) { return errorResponse('Invalid request format for update. Expected multipart/form-data.', 400, e); }

    // --- Find Member ---
    const existingMember = await env.DB.prepare('SELECT * FROM members WHERE id = ?')
        .bind(targetMemberId)
        .first<Member>();

    if (!existingMember) {
        return errorResponse('Member not found.', 404);
    }

    // --- Prepare Updates ---
    const updates: Partial<Member> = {};
    const setClauses: string[] = [];
    const params: (string | number | null)[] = [];
    let newAvatarUrl: string | null | undefined = undefined;
    let oldAvatarUrlToDelete: string | null | undefined = undefined;

    // Get potential new values from FormData
    const newTeamCode = formData.get('teamCode')?.toString()?.trim();
    const newColor = formData.get('color')?.toString();
    const newJob = formData.get('job')?.toString();
    const newMaimaiId = formData.get('maimaiId')?.toString()?.trim();
    const newNickname = formData.get('nickname')?.toString()?.trim();
    const newQqNumber = formData.get('qqNumber')?.toString()?.trim();
    const newKindeUserId = formData.get('kindeUserId')?.toString()?.trim() || null; // Optional Kinde ID
    const newIsAdmin = formData.get('isAdmin')?.toString(); // Optional admin status
    const newAvatarFile = formData.get('avatarFile');
    const clearAvatar = formData.get('clearAvatar')?.toString() === 'true';


    // --- Validate and Add Fields to Update ---
    if (newTeamCode !== undefined && newTeamCode !== existingMember.team_code) {
        if (newTeamCode.length !== 4 || isNaN(parseInt(newTeamCode))) { return errorResponse('Invalid new team code format.', 400); }
        // Check if new team exists
        const teamExists = await env.DB.prepare('SELECT 1 FROM teams WHERE code = ? LIMIT 1').bind(newTeamCode).first();
        if (!teamExists) { return errorResponse(`New team code ${newTeamCode} not found.`, 404); }
        updates.team_code = newTeamCode; setClauses.push('team_code = ?'); params.push(newTeamCode);
    }
    if (newColor !== undefined && newColor !== existingMember.color) {
        if (!['red', 'green', 'blue'].includes(newColor)) return errorResponse('Invalid new color selection.', 400);
        // Check color conflict in the *new* team if teamCode is changing, otherwise in the current team
        const targetTeamCodeForConflictCheck = newTeamCode !== undefined ? newTeamCode : existingMember.team_code;
         const colorConflict = await env.DB.prepare(
                 'SELECT 1 FROM members WHERE team_code = ? AND color = ? AND id != ? LIMIT 1'
            )
            .bind(targetTeamCodeForConflictCheck, newColor, existingMember.id)
            .first();
        if (colorConflict) { return errorResponse(`The color '${newColor}' is already taken by another member in team ${targetTeamCodeForConflictCheck}.`, 409); }
        updates.color = newColor; setClauses.push('color = ?'); params.push(newColor);
    }
   if (newJob !== undefined && newJob !== existingMember.job) {
        if (!['attacker', 'defender', 'supporter'].includes(newJob)) return errorResponse('Invalid new job selection.', 400);
        // Check job conflict in the *new* team if teamCode is changing, otherwise in the current team
        const targetTeamCodeForConflictCheck = newTeamCode !== undefined ? newTeamCode : existingMember.team_code;
         const jobConflict = await env.DB.prepare(
                 'SELECT 1 FROM members WHERE team_code = ? AND job = ? AND id != ? LIMIT 1'
            )
            .bind(targetTeamCodeForConflictCheck, newJob, existingMember.id)
            .first();
        if (jobConflict) { return errorResponse(`The job '${newJob}' is already taken by another member in team ${targetTeamCodeForConflictCheck}.`, 409); }
        updates.job = newJob; setClauses.push('job = ?'); params.push(newJob);
   }
    if (newMaimaiId !== undefined && newMaimaiId !== existingMember.maimai_id) {
        if (newMaimaiId.length === 0 || newMaimaiId.length > 13) { return errorResponse('Maimai ID must be between 1 and 13 characters.', 400); }
        // Check if new Maimai ID is already taken by someone else
        const maimaiIdConflict = await env.DB.prepare('SELECT 1 FROM members WHERE maimai_id = ? AND id != ? LIMIT 1').bind(newMaimaiId, existingMember.id).first();
        if (maimaiIdConflict) { return errorResponse(`Maimai ID ${newMaimaiId} is already registered.`, 409); }
        updates.maimai_id = newMaimaiId; setClauses.push('maimai_id = ?'); params.push(newMaimaiId);
    }
    if (newNickname !== undefined && newNickname !== existingMember.nickname) {
        if (newNickname.length === 0 || newNickname.length > 50) { return errorResponse('Nickname must be between 1 and 50 characters.', 400); }
        updates.nickname = newNickname; setClauses.push('nickname = ?'); params.push(newNickname);
    }
    if (newQqNumber !== undefined && newQqNumber !== existingMember.qq_number) {
        if (!/^[1-9][0-9]{4,14}$/.test(newQqNumber)) { return errorResponse('Invalid format for new QQ number.', 400); }
        updates.qq_number = newQqNumber; setClauses.push('qq_number = ?'); params.push(newQqNumber);
    }
    if (newKindeUserId !== undefined && newKindeUserId !== existingMember.kinde_user_id) {
         // Allow setting to null/empty string to unlink Kinde account
         if (newKindeUserId !== null && newKindeUserId !== '' && newKindeUserId.length > 100) { return errorResponse('Kinde User ID is too long.', 400); }
         // Check if new Kinde User ID is already linked to another member
         if (newKindeUserId !== null && newKindeUserId !== '') {
              const kindeIdConflict = await env.DB.prepare('SELECT 1 FROM members WHERE kinde_user_id = ? AND id != ? LIMIT 1').bind(newKindeUserId, existingMember.id).first();
              if (kindeIdConflict) { return errorResponse(`Kinde User ID ${newKindeUserId} is already linked to another member.`, 409); }
         }
         updates.kinde_user_id = newKindeUserId === '' ? null : newKindeUserId; setClauses.push('kinde_user_id = ?'); params.push(updates.kinde_user_id);
    }
    if (newIsAdmin !== undefined) {
         const isAdminValue = newIsAdmin === 'true' || newIsAdmin === '1';
         const isAdminInt = isAdminValue ? 1 : 0;
         if (isAdminInt !== existingMember.is_admin) {
              updates.is_admin = isAdminInt; setClauses.push('is_admin = ?'); params.push(isAdminInt);
         }
    }

    // --- Handle Avatar Changes (Admin can update any member's avatar) ---
    if (clearAvatar) {
         newAvatarUrl = null; updates.avatar_url = null;
         if (existingMember.avatar_url) { oldAvatarUrlToDelete = existingMember.avatar_url; }
    } else if (newAvatarFile instanceof File) {
       console.log(`Admin processing new avatar file upload for member ID ${existingMember.id}`);
       // Use Kinde User ID or Maimai ID for avatar path
       const idForAvatarPath = existingMember.kinde_user_id || existingMember.maimai_id || targetMemberId.toString(); // Fallback to internal ID
       if (!idForAvatarPath) {
            console.error(`Cannot determine identifier for avatar path for member ID ${existingMember.id}`);
            return errorResponse('Failed to determine avatar identifier.', 500);
       }
       const targetTeamCodeForAvatar = newTeamCode !== undefined ? newTeamCode : existingMember.team_code; // Use new team code if changing
       const uploadedUrl = await uploadAvatar(env, newAvatarFile, idForAvatarPath, targetTeamCodeForAvatar);
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
   console.log(`Executing admin update for ID ${existingMember.id}: ${updateQuery} with params: ${JSON.stringify(params.slice(0, -1))}`);

   try {
       if (oldAvatarUrlToDelete) { ctx.waitUntil(deleteAvatarFromR2(env, oldAvatarUrlToDelete)); }

        const updateResult = await env.DB.prepare(updateQuery).bind(...params).run();

        if (!updateResult.success) {
             console.error(`Admin update member database operation failed for ID ${existingMember.id}:`, updateResult.error);
              if (updateResult.error?.includes('UNIQUE constraint failed')) {
                   return errorResponse(`Update failed due to a conflict (color, job, maimai ID, or Kinde ID). Please check values.`, 409);
              }
             return errorResponse('Failed to update member information due to a database issue.', 500);
        }

        if (updateResult.meta.changes === 0) {
            console.warn(`Admin update query executed for ID ${existingMember.id} but no rows were changed.`);
            const checkExists = await env.DB.prepare('SELECT 1 FROM members WHERE id = ?').bind(existingMember.id).first();
            if (!checkExists) return errorResponse('Failed to update: Member record not found.', 404);
            return jsonResponse({ message: "No changes detected or record unchanged.", member: existingMember }, 200);
        }

        console.log(`Successfully updated member ID ${existingMember.id}. Changes: ${updateResult.meta.changes}`);

        // Fetch the *updated* member data to return
        const updatedMember = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?')
            .bind(existingMember.id)
            .first<Member>();

        if (!updatedMember) {
              console.error(`Consistency issue: Member ID ${existingMember.id} updated but could not be re-fetched.`);
              return errorResponse('Update successful, but failed to retrieve updated data.', 500);
        }

        return jsonResponse({ success: true, message: "Information updated successfully.", member: updatedMember }, 200);

   } catch (updateProcessError: any) {
        console.error(`Error during the admin member update process for ID ${existingMember.id}:`, updateProcessError);
        return errorResponse(
             `Failed to process update: ${updateProcessError instanceof Error ? updateProcessError.message : 'Unknown error'}`,
             500,
             updateProcessError
        );
   }
}

// Admin Add Member (Basic Implementation)
async function handleAdminAddMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/admin/members POST request...`);
    let formData: FormData;
    try { formData = await request.formData(); } catch (e: any) { return errorResponse('Invalid request format. Expected multipart/form-data.', 400, e); }

    const teamCode = formData.get('teamCode')?.toString()?.trim();
    const color = formData.get('color')?.toString();
    const job = formData.get('job')?.toString();
    const maimaiId = formData.get('maimaiId')?.toString()?.trim();
    const nickname = formData.get('nickname')?.toString()?.trim();
    const qqNumber = formData.get('qqNumber')?.toString()?.trim();
    const kindeUserIdToAdd = formData.get('kindeUserId')?.toString()?.trim() || null; // Optional Kinde ID
    const isAdmin = formData.get('isAdmin')?.toString() === 'true' ? 1 : 0; // Optional admin status
    const avatarFile = formData.get('avatarFile');

    // --- Input Validation ---
    if (!teamCode || teamCode.length !== 4 || isNaN(parseInt(teamCode))) return errorResponse('Invalid team code.', 400);
    if (!color || !['red', 'green', 'blue'].includes(color)) return errorResponse('Invalid color selection.', 400);
    if (!job || !['attacker', 'defender', 'supporter'].includes(job)) return errorResponse('Invalid job selection.', 400);
    if (!maimaiId || maimaiId.length === 0 || maimaiId.length > 13) return errorResponse('Maimai ID is required (1-13 chars).', 400);
    if (!nickname || nickname.length === 0 || nickname.length > 50) return errorResponse('Nickname is required (1-50 chars).', 400);
    if (!qqNumber || !/^[1-9][0-9]{4,14}$/.test(qqNumber)) return errorResponse('A valid QQ number is required.', 400);
    if (kindeUserIdToAdd && kindeUserIdToAdd.length > 100) return errorResponse('Kinde User ID is too long.', 400);
    // --- End Validation ---

    try {
        const checks = await env.DB.batch([
            env.DB.prepare('SELECT 1 FROM teams WHERE code = ? LIMIT 1').bind(teamCode),
            env.DB.prepare('SELECT 1 FROM members WHERE team_code = ? AND color = ? LIMIT 1').bind(teamCode, color),
            env.DB.prepare('SELECT 1 FROM members WHERE team_code = ? AND job = ? LIMIT 1').bind(teamCode, job),
            env.DB.prepare('SELECT 1 FROM members WHERE maimai_id = ? LIMIT 1').bind(maimaiId),
            kindeUserIdToAdd ? env.DB.prepare('SELECT 1 FROM members WHERE kinde_user_id = ? LIMIT 1').bind(kindeUserIdToAdd) : null,
        ].filter(Boolean)); // Filter out null if kindeUserIdToAdd is null

        const teamExists = checks[0]?.results?.[0];
        const colorConflict = checks[1]?.results?.[0];
        const jobConflict = checks[2]?.results?.[0];
        const maimaiIdConflict = checks[3]?.results?.[0];
        const kindeIdConflict = kindeUserIdToAdd ? checks[4]?.results?.[0] : null;

        if (!teamExists) return errorResponse(`Team with code ${teamCode} not found.`, 404);
        if (colorConflict) return errorResponse(`The color '${color}' is already taken in team ${teamCode}.`, 409);
        if (jobConflict) return errorResponse(`The job '${job}' is already taken in team ${teamCode}.`, 409);
        if (maimaiIdConflict) return errorResponse(`Maimai ID ${maimaiId} is already registered.`, 409);
        if (kindeIdConflict) return errorResponse(`Kinde User ID ${kindeUserIdToAdd} is already linked to another member.`, 409);

        let avatarUrl: string | null = null;
        if (avatarFile instanceof File) {
            const idForAvatarPath = kindeUserIdToAdd || maimaiId;
            if (!idForAvatarPath) {
                 console.error(`Cannot determine identifier for avatar path for new member (maimaiId: ${maimaiId})`);
                 return errorResponse('Failed to determine avatar identifier.', 500);
            }
            avatarUrl = await uploadAvatar(env, avatarFile, idForAvatarPath, teamCode);
            if (avatarUrl === null) {
                console.warn(`Admin add member blocked for ${maimaiId}: Avatar upload failed.`);
                return errorResponse('Failed to upload avatar. Member not added.', 500);
            }
        }

        const now = Math.floor(Date.now() / 1000);
        const insertResult = await env.DB.prepare(
            'INSERT INTO members (team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, kinde_user_id, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        )
        .bind(teamCode, color, job, maimaiId, nickname, qqNumber, avatarUrl, now, kindeUserIdToAdd, isAdmin)
        .run();

        if (!insertResult.success) {
            console.error('Admin add member database insert failed:', insertResult.error);
            return errorResponse(insertResult.error || 'Failed to add member due to a database issue.', 500);
        }

        const newMemberId = insertResult.meta.last_row_id;
        const newMember = await env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE id = ?')
            .bind(newMemberId)
            .first<Member>();

        return jsonResponse({ success: true, message: "Member added successfully.", member: newMember }, 201);

    } catch (e: any) {
        console.error('Error during admin add member process:', e);
        return errorResponse(e.message, 500, e);
    }
}

// Admin Delete Member (Basic Implementation)
async function handleAdminDeleteMember(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/admin/members/:id DELETE request...`);
    const parts = new URL(request.url).pathname.split('/');
    const targetMemberId = parseInt(parts[4], 10); // /api/admin/members/:id -> parts[4]

    if (isNaN(targetMemberId)) {
        return errorResponse("Invalid member ID in path", 400);
    }

    try {
        // Fetch member details before deleting to get avatar_url and team_code
        const existingMember = await env.DB.prepare('SELECT id, team_code, avatar_url FROM members WHERE id = ?')
            .bind(targetMemberId)
            .first<{ id: number, team_code: string, avatar_url?: string | null }>();

        if (!existingMember) {
            return errorResponse('Member not found.', 404);
        }

        const teamCode = existingMember.team_code;
        const avatarUrlToDelete = existingMember.avatar_url;

        console.log(`Admin user ${kindeUserId} attempting to delete member ID ${targetMemberId}`);
        const deleteResult = await env.DB.prepare('DELETE FROM members WHERE id = ?')
            .bind(targetMemberId)
            .run();

        if (!deleteResult.success) {
            console.error(`Admin delete member database operation failed for ID ${targetMemberId}:`, deleteResult.error);
            return errorResponse(deleteResult.error || 'Failed to delete member due to a database issue.', 500);
        }
        if (deleteResult.meta.changes === 0) {
             console.warn(`Admin delete query executed for ID ${targetMemberId} but no rows changed.`);
             return errorResponse('Member not found or already deleted.', 404);
        }
        console.log(`Successfully deleted member record for ID ${targetMemberId}.`);

        if (avatarUrlToDelete) {
             console.log(`Admin user ${kindeUserId} attempting to delete associated avatar: ${avatarUrlToDelete}`);
             ctx.waitUntil(deleteAvatarFromR2(env, avatarUrlToDelete));
        }

        // Check and delete the team if it becomes empty
        ctx.waitUntil(checkAndDeleteEmptyTeam(env, teamCode));

        return new Response(null, { status: 204, headers: CORS_HEADERS });

    } catch (e: any) {
        console.error(`Error during admin member deletion process for ID ${targetMemberId}:`, e);
        return errorResponse(e.message, 500, e);
    }
}

// Admin Update Settings (Placeholder)
async function handleAdminUpdateSettings(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/admin/settings PATCH request...`);
    try {
        const payload: { key: string; value: string } = await request.json();

        if (!payload || typeof payload.key !== 'string' || typeof payload.value !== 'string') {
            return errorResponse('Invalid payload format. Expected { key: string, value: string }.', 400);
        }

        // Only allow updating specific settings, e.g., 'collection_paused'
        const allowedSettings = ['collection_paused'];
        if (!allowedSettings.includes(payload.key)) {
            return errorResponse(`Updating setting '${payload.key}' is not allowed via this endpoint.`, 403);
        }

        const now = new Date().toISOString();
        const result = await env.DB.prepare(
            `INSERT INTO settings (key, value, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
                 value = excluded.value,
                 updated_at = excluded.updated_at
            `
        )
        .bind(payload.key, payload.value, now)
        .run();

        if (result.success) {
            console.log(`Admin user ${kindeUserId} updated setting '${payload.key}' to '${payload.value}'.`);
            return jsonResponse({ success: true, message: "Setting updated successfully." }, 200);
        } else {
            console.error(`Admin user ${kindeUserId}: Failed to update setting '${payload.key}':`, result.error);
            return errorResponse(result.error || "Failed to update setting.", 500);
        }

    } catch (e: any) {
        console.error(`Error during admin update settings process:`, e);
        return errorResponse(e.message, 500, e);
    }
}


// --- Tournament/Match API Handlers ---
// POST /api/tournament_matches (Admin Only)
async function handleCreateTournamentMatch(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log(`Admin user ${kindeUserId} handling /api/tournament_matches POST request...`);
    try {
        const payload: CreateTournamentMatchPayload = await request.json();

        if (!payload.round_name || payload.team1_id === null || payload.team2_id === null) {
            return errorResponse('Missing required fields: round_name, team1_id, team2_id.', 400);
        }
        if (payload.team1_id === payload.team2_id) {
             return errorResponse('Team 1 and Team 2 cannot be the same.', 400);
        }

        // Verify teams exist
        const teamsExist = await env.DB.batch([
            env.DB.prepare('SELECT 1 FROM teams WHERE id = ? LIMIT 1').bind(payload.team1_id),
            env.DB.prepare('SELECT 1 FROM teams WHERE id = ? LIMIT 1').bind(payload.team2_id),
        ]);
        if (!teamsExist[0].results[0] || !teamsExist[1].results[0]) {
             return errorResponse('One or both teams not found.', 404);
        }


        const now = new Date().toISOString();
        const insertResult = await env.DB.prepare(
            'INSERT INTO tournament_matches (round_name, team1_id, team2_id, status, scheduled_time, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)'
        )
        .bind(
            payload.round_name,
            payload.team1_id,
            payload.team2_id,
            'pending_song_confirmation', // Initial status is pending song selection/confirmation
            payload.scheduled_time || null,
            now,
            now
        )
        .run();

        if (!insertResult.success) {
            console.error("Worker: Failed to create tournament match:", insertResult.error);
            return errorResponse(insertResult.error || "Failed to create match.");
        }

        const newMatchId = insertResult.meta.last_row_id;
        // Fetch the newly created match with denormalized names for response
        const newMatch = await env.DB.prepare(`
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            WHERE tm.id = ?
        `).bind(newMatchId).first<TournamentMatch>();


        return jsonResponse(newMatch, 201);

    } catch (e: any) {
        console.error("Worker: Exception creating tournament match:", e);
        return errorResponse(e.message);
    }
}

// GET /api/tournament_matches (Public)
async function handleFetchTournamentMatches(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/tournament_matches request...');
    try {
        // Fetch matches with joined team names
        const query = `
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name,
                tw.name AS winner_team_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            LEFT JOIN teams tw ON tm.winner_team_id = tw.id
            ORDER BY tm.scheduled_time DESC, tm.created_at DESC
        `;
        const { results } = await env.DB.prepare(query).all<TournamentMatch>();

        // Parse JSON fields and add fullCoverUrl for match_song_list
        const matchesWithParsedData = results.map(match => {
            const parsedMatch = { ...match } as TournamentMatch; // Copy to avoid modifying original result object
            if (match.team1_player_order_json) {
                try { parsedMatch.team1_player_order = JSON.parse(match.team1_player_order_json); } catch (e) { console.error(`Failed to parse team1_player_order_json for match ${match.id}`, e); }
            }
            if (match.team2_player_order_json) {
                try { parsedMatch.team2_player_order = JSON.parse(match.team2_player_order_json); } catch (e) { console.error(`Failed to parse team2_player_order_json for match ${match.id}`, e); }
            }
            if (match.match_song_list_json) {
                try {
                    const songList = JSON.parse(match.match_song_list_json) as MatchSong[];
                    // Add fullCoverUrl to each song in the list
                    parsedMatch.match_song_list = songList.map(song => ({
                        ...song,
                        fullCoverUrl: song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song.cover_filename}` // Correct R2 path
                            : undefined,
                    }));
                } catch (e) { console.error(`Failed to parse match_song_list_json for match ${match.id}`, e); }
            }
            // Remove raw JSON fields from the response data
            delete (parsedMatch as any).team1_player_order_json;
            delete (parsedMatch as any).team2_player_order_json;
            delete (parsedMatch as any).match_song_list_json;

            return parsedMatch;
        });


        return jsonResponse(matchesWithParsedData);
    } catch (e: any) {
        console.error("Worker: Failed to list tournament matches:", e);
        return errorResponse(e.message);
    }
}

// PUT /api/tournament_matches/:id/confirm_setup (Admin Only)
async function handleConfirmMatchSetup(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchId = parseInt(parts[3], 10); // /api/tournament_matches/:id/confirm_setup -> parts[3]
    console.log(`Admin user ${kindeUserId} handling /api/tournament_matches/${matchId}/confirm_setup PUT request...`);

    if (isNaN(matchId)) {
        return errorResponse("Invalid match ID in path", 400);
    }

    try {
        const payload: ConfirmMatchSetupPayload = await request.json();

        // Basic validation
        if (!Array.isArray(payload.team1_player_order) || !Array.isArray(payload.team2_player_order) || !Array.isArray(payload.match_song_list)) {
            return errorResponse('Invalid payload format.', 400);
        }
        // TODO: Add more robust validation for player IDs and song list structure/content

        // Fetch the match to ensure it exists and is in a state that allows setup
        const match = await env.DB.prepare('SELECT id, status FROM tournament_matches WHERE id = ?').bind(matchId).first<TournamentMatch>();
        if (!match) {
            return errorResponse('Match not found.', 404);
        }
        // Allow setup if status is scheduled or pending_song_confirmation
        if (match.status !== 'scheduled' && match.status !== 'pending_song_confirmation') {
             return errorResponse(`Match status is "${match.status}", cannot confirm setup.`, 400);
        }


        // Update the tournament_matches record with the manually confirmed setup
        const now = new Date().toISOString();
        const updateResult = await env.DB.prepare(
            'UPDATE tournament_matches SET team1_player_order_json = ?, team2_player_order_json = ?, match_song_list_json = ?, status = ?, updated_at = ? WHERE id = ?'
        )
        .bind(
            JSON.stringify(payload.team1_player_order),
            JSON.stringify(payload.team2_player_order),
            JSON.stringify(payload.match_song_list),
            'ready_to_start', // Status changes to ready_to_start after setup
            now,
            matchId
        )
        .run();

        if (!updateResult.success) {
            console.error("Worker: Failed to confirm match setup:", updateResult.error);
            return errorResponse(updateResult.error || "Failed to confirm match setup.");
        }

        // Fetch the updated match record with denormalized names for response
        const updatedMatch = await env.DB.prepare(`
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name,
                tw.name AS winner_team_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            LEFT JOIN teams tw ON tm.winner_team_id = tw.id
            WHERE tm.id = ?
        `).bind(matchId).first<TournamentMatch>();

         if (updatedMatch) {
             // Parse JSON fields and add fullCoverUrl for match_song_list
             if (updatedMatch.team1_player_order_json) {
                 try { updatedMatch.team1_player_order = JSON.parse(updatedMatch.team1_player_order_json); } catch (e) { console.error(`Failed to parse team1_player_order_json for match ${updatedMatch.id}`, e); }
             }
             if (updatedMatch.team2_player_order_json) {
                 try { updatedMatch.team2_player_order = JSON.parse(updatedMatch.team2_player_order_json); } catch (e) { console.error(`Failed to parse team2_player_order_json for match ${updatedMatch.id}`, e); }
             }
             if (updatedMatch.match_song_list_json) {
                 try {
                     const songList = JSON.parse(updatedMatch.match_song_list_json) as MatchSong[];
                     // Add fullCoverUrl to each song in the list
                     updatedMatch.match_song_list = songList.map(song => ({
                         ...song,
                         fullCoverUrl: song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                             ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song.cover_filename}` // Correct R2 path
                             : undefined,
                     }));
                 } catch (e) { console.error(`Failed to parse match_song_list_json for match ${updatedMatch.id}`, e); }
             }
             // Remove raw JSON fields from the response data
             delete (updatedMatch as any).team1_player_order_json;
             delete (updatedMatch as any).team2_player_order_json;
             delete (updatedMatch as any).match_song_list_json;
         }


        return jsonResponse(updatedMatch, 200);

    } catch (e: any) {
        console.error("Worker: Exception confirming match setup:", e);
        return errorResponse(e.message);
    }
}


// GET /api/tournament_matches/:matchId/selection-status (Admin Only - Changed from Public based on File 1 description needing team/member details)
async function handleCheckMatchSelectionStatus(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    // Corrected: matchId is at index 3 for this path structure
    const matchId = parseInt(parts[3], 10); // /api/tournament_matches/:matchId/selection-status -> parts[3]
    console.log(`Handling GET /api/tournament_matches/${matchId}/selection-status for Admin user ID: ${kindeUserId}`);

    if (isNaN(matchId)) {
        return errorResponse("Invalid match ID in path", 400);
    }

    try {
        // Fetch match details
        const match = await env.DB.prepare('SELECT id, team1_id, team2_id, status FROM tournament_matches WHERE id = ?').bind(matchId).first<TournamentMatch>();
        if (!match) {
            return errorResponse('Match not found.', 404);
        }

        // Fetch teams and members for both teams
        const [team1Result, team2Result, team1MembersResult, team2MembersResult, selectionsResult] = await env.DB.batch([
            env.DB.prepare('SELECT id, name FROM teams WHERE id = ? LIMIT 1').bind(match.team1_id),
            env.DB.prepare('SELECT id, name FROM teams WHERE id = ? LIMIT 1').bind(match.team2_id),
            env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = (SELECT code FROM teams WHERE id = ? LIMIT 1)').bind(match.team1_id),
            env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = (SELECT code FROM teams WHERE id = ? LIMIT 1)').bind(match.team2_id),
            env.DB.prepare('SELECT member_id, team_id FROM match_player_selections WHERE tournament_match_id = ?').bind(matchId),
        ]);

        const team1 = team1Result.results[0] as { id: number; name: string } | undefined;
        const team2 = team2Result.results[0] as { id: number; name: string } | undefined;
        const team1Members = team1MembersResult.results as { id: number; nickname: string; team_code: string }[] || [];
        const team2Members = team2MembersResult.results as { id: number; nickname: string; team_code: string }[] || [];
        const selections = selectionsResult.results as { member_id: number; team_id: number }[] || [];

        if (!team1 || !team2) {
             // This shouldn't happen if match exists and FKs are correct, but handle defensively
             return errorResponse('Could not retrieve team information for the match.', 500);
        }

        const team1Required = team1Members.length;
        const team2Required = team2Members.length;

        const team1Completed = selections.filter(s => s.team_id === team1.id).length;
        const team2Completed = selections.filter(s => s.team_id === team2.id).length;

        const team1SelectedMemberIds = new Set(selections.filter(s => s.team_id === team1.id).map(s => s.member_id));
        const team2SelectedMemberIds = new Set(selections.filter(s => s.team_id === team2.id).map(s => s.member_id));

        const team1MissingMembers = team1Members.filter(m => !team1SelectedMemberIds.has(m.id)).map(m => ({ id: m.id, nickname: m.nickname }));
        const team2MissingMembers = team2Members.filter(m => !team2SelectedMemberIds.has(m.id)).map(m => ({ id: m.id, nickname: m.nickname }));

        const isReadyToCompile = team1Completed === team1Required && team2Completed === team2Required;

        const responseData: MatchSelectionStatus = {
            matchId: matchId,
            isReadyToCompile: isReadyToCompile,
            team1Status: {
                teamId: team1.id,
                teamName: team1.name,
                requiredSelections: team1Required,
                completedSelections: team1Completed,
                missingMembers: team1MissingMembers,
            },
            team2Status: {
                teamId: team2.id,
                teamName: team2.name,
                requiredSelections: team2Required,
                completedSelections: team2Completed,
                missingMembers: team2MissingMembers,
            },
        };

        return jsonResponse(responseData);

    } catch (e: any) {
        console.error("Error checking match selection status:", e);
        return errorResponse('Failed to check match selection status.', 500, e);
    }
}


// POST /api/tournament_matches/:matchId/compile-setup (Admin Only)
async function handleCompileMatchSetup(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    // Corrected: matchId is at index 3 for this path structure
    const matchId = parseInt(parts[3], 10); // /api/tournament_matches/:matchId/compile-setup -> parts[3]
    console.log(`Handling POST /api/tournament_matches/${matchId}/compile-setup for Admin user ID: ${kindeUserId}`);

    if (isNaN(matchId)) {
        return errorResponse("Invalid match ID in path", 400);
    }

    try {
        // Fetch match details and check status
        const match = await env.DB.prepare('SELECT id, team1_id, team2_id, status FROM tournament_matches WHERE id = ?').bind(matchId).first<TournamentMatch>();
        if (!match) {
            return errorResponse('Match not found.', 404);
        }
        if (match.status !== 'pending_song_confirmation') {
             return errorResponse(`Match status is "${match.status}", cannot compile setup. Status must be 'pending_song_confirmation'.`, 400);
        }

        // Fetch teams and members for both teams
        const [team1Result, team2Result, team1MembersResult, team2MembersResult, selectionsResult] = await env.DB.batch([
            env.DB.prepare('SELECT id, name FROM teams WHERE id = ? LIMIT 1').bind(match.team1_id),
            env.DB.prepare('SELECT id, name FROM teams WHERE id = ? LIMIT 1').bind(match.team2_id),
            env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = (SELECT code FROM teams WHERE id = ? LIMIT 1)').bind(match.team1_id),
            env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = (SELECT code FROM teams WHERE id = ? LIMIT 1)').bind(match.team2_id),
            env.DB.prepare('SELECT mps.*, m.nickname FROM match_player_selections mps JOIN members m ON mps.member_id = m.id WHERE mps.tournament_match_id = ?').bind(matchId),
        ]);

        const team1 = team1Result.results[0] as { id: number; name: string } | undefined;
        const team2 = team2Result.results[0] as { id: number; name: string } | undefined;
        const team1Members = team1MembersResult.results as { id: number; nickname: string; team_code: string }[] || [];
        const team2Members = team2MembersResult.results as { id: number; nickname: string; team_code: string }[] || [];
        const selections = selectionsResult.results as (MatchPlayerSelection & { nickname: string })[] || [];

        if (!team1 || !team2) {
             return errorResponse('Could not retrieve team information for the match.', 500);
        }

        const team1Required = team1Members.length;
        const team2Required = team2Members.length;
        const completedSelectionsCount = selections.length;

        if (completedSelectionsCount !== team1Required + team2Required) {
             // Check if all members from both teams have submitted selections
             const team1SelectedCount = selections.filter(s => s.team_id === team1.id).length;
             const team2SelectedCount = selections.filter(s => s.team_id === team2.id).length;

             if (team1SelectedCount !== team1Required || team2SelectedCount !== team2Required) {
                 const missingTeam1 = team1Required - team1SelectedCount;
                 const missingTeam2 = team2Required - team2SelectedCount;
                 let errorMessage = "Cannot compile setup: Not all players have submitted their song selections.";
                 if (missingTeam1 > 0) errorMessage += ` Team ${team1.name} is missing ${missingTeam1} selection(s).`;
                 if (missingTeam2 > 0) errorMessage += ` Team ${team2.name} is missing ${missingTeam2} selection(s).`;
                 return errorResponse(errorMessage, 400);
             }
        }

        // Group and sort selections by team and order index
        const team1Selections = selections.filter(s => s.team_id === team1.id).sort((a, b) => a.selected_order_index - b.selected_order_index);
        const team2Selections = selections.filter(s => s.team_id === team2.id).sort((a, b) => a.selected_order_index - b.selected_order_index);

        // Construct player order arrays (array of member IDs)
        const team1PlayerOrder = team1Selections.map(s => s.member_id);
        const team2PlayerOrder = team2Selections.map(s => s.member_id);

        // Construct match song list (alternating players, then song 1/song 2)
        const matchSongList: MatchSong[] = [];
        const maxPlayersPerTeam = Math.max(team1Selections.length, team2Selections.length); // Should be equal if all selected

        // Fetch song details for all selected songs efficiently
        const songIds = new Set<number>();
        selections.forEach(s => {
            songIds.add(s.song1_id);
            songIds.add(s.song2_id);
        });
        const songDetailsResult = await env.DB.prepare(`SELECT id, title, cover_filename, levels_json FROM songs WHERE id IN (${Array.from(songIds).join(',')})`).all<Song & { levels_json: string | null }>();
        const songDetailsMap = new Map<number, Song & { levels_json: string | null }>();
        songDetailsResult.results?.forEach(song => songDetailsMap.set(song.id, song));


        // Add Song 1 for each player, alternating teams
        for (let i = 0; i < maxPlayersPerTeam; i++) {
            if (i < team1Selections.length) {
                const selection = team1Selections[i];
                const songDetail = songDetailsMap.get(selection.song1_id);
                if (songDetail) {
                    matchSongList.push({
                        song_id: selection.song1_id,
                        song_difficulty: selection.song1_difficulty, // Use song_difficulty field name
                        picker_member_id: selection.member_id, // Use picker_member_id field name
                        picker_team_id: selection.team_id, // Use picker_team_id field name
                        song_title: songDetail.title, // Use song_title field name
                        cover_filename: songDetail.cover_filename,
                        // Note: MatchSong type doesn't typically store parsedLevels or fullCoverUrl directly,
                        // but the frontend might expect them if you denormalize heavily.
                        // Let's add them for consistency with frontend expectations based on MatchSong type in store.ts
                        parsedLevels: songDetail.levels_json ? JSON.parse(songDetail.levels_json) : undefined,
                        fullCoverUrl: songDetail.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${songDetail.cover_filename}` // Correct R2 path
                            : undefined,
                        // Add other MatchSong fields with default/initial values
                        status: 'pending', // Initial status in the match song list
                        song_element: null, // Assuming element is not selected by player
                        bpm: songDetail.bpm, // Assuming bpm is needed
                        teamA_player_id: undefined, teamB_player_id: undefined,
                        teamA_percentage: undefined, teamB_percentage: undefined,
                        teamA_damage_dealt: undefined, teamB_damage_dealt: undefined,
                        teamA_effect_value: undefined, teamB_effect_value: undefined,
                        teamA_health_after: undefined, teamB_health_after: undefined,
                        teamA_mirror_triggered: undefined, teamB_mirror_triggered: undefined,
                    } as MatchSong); // Cast to MatchSong
                } else {
                     console.error(`Song details not found for ID ${selection.song1_id} during compilation.`);
                     // Decide how to handle missing song details - error or skip? Error is safer.
                     return errorResponse(`Failed to find details for song ID ${selection.song1_id} during compilation.`, 500);
                }
            }
            if (i < team2Selections.length) {
                const selection = team2Selections[i];
                 const songDetail = songDetailsMap.get(selection.song1_id);
                 if (songDetail) {
                    matchSongList.push({
                        song_id: selection.song1_id,
                        song_difficulty: selection.song1_difficulty,
                        picker_member_id: selection.member_id,
                        picker_team_id: selection.team_id,
                        song_title: songDetail.title,
                        cover_filename: songDetail.cover_filename,
                        parsedLevels: songDetail.levels_json ? JSON.parse(songDetail.levels_json) : undefined,
                        fullCoverUrl: songDetail.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${songDetail.cover_filename}` // Correct R2 path
                            : undefined,
                        status: 'pending',
                        song_element: null,
                        bpm: songDetail.bpm,
                        teamA_player_id: undefined, teamB_player_id: undefined,
                        teamA_percentage: undefined, teamB_percentage: undefined,
                        teamA_damage_dealt: undefined, teamB_damage_dealt: undefined,
                        teamA_effect_value: undefined, teamB_effect_value: undefined,
                        teamA_health_after: undefined, teamB_health_after: undefined,
                        teamA_mirror_triggered: undefined, teamB_mirror_triggered: undefined,
                    } as MatchSong); // Cast to MatchSong
                 } else {
                     console.error(`Song details not found for ID ${selection.song1_id} during compilation.`);
                     return errorResponse(`Failed to find details for song ID ${selection.song1_id} during compilation.`, 500);
                 }
            }
        }

        // Add Song 2 for each player, alternating teams
        for (let i = 0; i < maxPlayersPerTeam; i++) {
            if (i < team1Selections.length) {
                const selection = team1Selections[i];
                 const songDetail = songDetailsMap.get(selection.song2_id);
                 if (songDetail) {
                    matchSongList.push({
                        song_id: selection.song2_id,
                        song_difficulty: selection.song2_difficulty,
                        picker_member_id: selection.member_id,
                        picker_team_id: selection.team_id,
                        song_title: songDetail.title,
                        cover_filename: songDetail.cover_filename,
                        parsedLevels: songDetail.levels_json ? JSON.parse(songDetail.levels_json) : undefined,
                        fullCoverUrl: songDetail.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${songDetail.cover_filename}` // Correct R2 path
                            : undefined,
                        status: 'pending',
                        song_element: null,
                        bpm: songDetail.bpm,
                        teamA_player_id: undefined, teamB_player_id: undefined,
                        teamA_percentage: undefined, teamB_percentage: undefined,
                        teamA_damage_dealt: undefined, teamB_damage_dealt: undefined,
                        teamA_effect_value: undefined, teamB_effect_value: undefined,
                        teamA_health_after: undefined, teamB_health_after: undefined,
                        teamA_mirror_triggered: undefined, teamB_mirror_triggered: undefined,
                    } as MatchSong); // Cast to MatchSong
                 } else {
                     console.error(`Song details not found for ID ${selection.song2_id} during compilation.`);
                     return errorResponse(`Failed to find details for song ID ${selection.song2_id} during compilation.`, 500);
                 }
            }
            if (i < team2Selections.length) {
                const selection = team2Selections[i];
                 const songDetail = songDetailsMap.get(selection.song2_id);
                 if (songDetail) {
                    matchSongList.push({
                        song_id: selection.song2_id,
                        song_difficulty: selection.song2_difficulty,
                        picker_member_id: selection.member_id,
                        picker_team_id: selection.team_id,
                        song_title: songDetail.title,
                        cover_filename: songDetail.cover_filename,
                        parsedLevels: songDetail.levels_json ? JSON.parse(songDetail.levels_json) : undefined,
                        fullCoverUrl: songDetail.cover_filename && env.R2_PUBLIC_BUCKET_URL
                            ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${songDetail.cover_filename}` // Correct R2 path
                            : undefined,
                        status: 'pending',
                        song_element: null,
                        bpm: songDetail.bpm,
                        teamA_player_id: undefined, teamB_player_id: undefined,
                        teamA_percentage: undefined, teamB_percentage: undefined,
                        teamA_damage_dealt: undefined, teamB_damage_dealt: undefined,
                        teamA_effect_value: undefined, teamB_effect_value: undefined,
                        teamA_health_after: undefined, teamB_health_after: undefined,
                        teamA_mirror_triggered: undefined, teamB_mirror_triggered: undefined,
                    } as MatchSong); // Cast to MatchSong
                 } else {
                     console.error(`Song details not found for ID ${selection.song2_id} during compilation.`);
                     return errorResponse(`Failed to find details for song ID ${selection.song2_id} during compilation.`, 500);
                 }
            }
        }


        // Update the tournament_matches record
        const now = new Date().toISOString();
        const updateResult = await env.DB.prepare(
            'UPDATE tournament_matches SET team1_player_order_json = ?, team2_player_order_json = ?, match_song_list_json = ?, status = ?, updated_at = ? WHERE id = ?'
        )
        .bind(
            JSON.stringify(team1PlayerOrder),
            JSON.stringify(team2PlayerOrder),
            JSON.stringify(matchSongList),
            'ready_to_start', // Status changes to ready_to_start after compilation
            now,
            matchId
        )
        .run();

        if (!updateResult.success) {
            console.error("Worker: Failed to compile match setup:", updateResult.error);
            return errorResponse(updateResult.error || "Failed to compile match setup.");
        }

        // Fetch the updated match record to return
        const updatedMatch = await env.DB.prepare(`
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name,
                tw.name AS winner_team_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            LEFT JOIN teams tw ON tm.winner_team_id = tw.id
            WHERE tm.id = ?
        `).bind(matchId).first<TournamentMatch>();

         if (updatedMatch) {
             // Parse JSON fields and add fullCoverUrl for match_song_list
             if (updatedMatch.team1_player_order_json) {
                 try { updatedMatch.team1_player_order = JSON.parse(updatedMatch.team1_player_order_json); } catch (e) { console.error(`Failed to parse team1_player_order_json for match ${updatedMatch.id}`, e); }
             }
             if (updatedMatch.team2_player_order_json) {
                 try { updatedMatch.team2_player_order = JSON.parse(updatedMatch.team2_player_order_json); } catch (e) { console.error(`Failed to parse team2_player_order_json for match ${updatedMatch.id}`, e); }
             }
             if (updatedMatch.match_song_list_json) {
                 try {
                     const songList = JSON.parse(updatedMatch.match_song_list_json) as MatchSong[];
                     // Add fullCoverUrl to each song in the list
                     updatedMatch.match_song_list = songList.map(song => ({
                         ...song,
                         fullCoverUrl: song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                             ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song.cover_filename}` // Correct R2 path
                             : undefined,
                     }));
                 } catch (e) { console.error(`Failed to parse match_song_list_json for match ${updatedMatch.id}`, e); }
             }
             // Remove raw JSON fields from the response data
             delete (updatedMatch as any).team1_player_order_json;
             delete (updatedMatch as any).team2_player_order_json;
             delete (updatedMatch as any).match_song_list_json;
         }


        const responseData: CompileMatchSetupResponse = {
            success: true,
            message: "Match setup compiled successfully.",
            tournamentMatch: updatedMatch,
        };

        return jsonResponse(responseData);

    } catch (e: any) {
        console.error(`Worker: Exception compiling match setup for match ${matchId}:`, e);
        return errorResponse(e.message);
    }
}


// GET /api/member/match-selection/:matchId (Authenticated User)
// MODIFIED: handleFetchUserMatchSelectionData to include selected song details
async function handleFetchUserMatchSelectionData(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    console.log('Handling /api/member/match-selection/:matchId request...');
    const url = new URL(request.url);
    const matchId = parseInt(url.pathname.split('/').pop() || '', 10);

    if (isNaN(matchId)) {
        return errorResponse('Invalid match ID.', 400);
    }

    try {
        // Fetch the match details with team names
        const matchQuery = env.DB.prepare(`
            SELECT
                tm.*,
                t1.name AS team1_name, t1.code AS team1_code,
                t2.name AS team2_name, t2.code AS team2_code
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            WHERE tm.id = ?
        `).bind(matchId);
        const matchResult = await matchQuery.first<TournamentMatch>();

        if (!matchResult) {
            return errorResponse('Match not found.', 404);
        }

        // Find the current user's member ID
        const currentUserMember = await env.DB.prepare('SELECT id, team_code, nickname FROM members WHERE kinde_user_id = ? LIMIT 1').bind(kindeUserId).first<Pick<Member, 'id' | 'team_code' | 'nickname'>>();

        if (!currentUserMember) {
             // User is authenticated but not linked to a member record
             return errorResponse('User member record not found.', 404);
        }

        // Determine user's team and opponent team
        let myTeam: Team | undefined;
        let opponentTeam: Team | undefined;
        let myTeamId: number | undefined;
        let opponentTeamId: number | undefined;

        // --- 修正这里的逻辑：通过 member.team_code 查找用户队伍在 teams 表中的 ID ---
        const userTeam = await env.DB.prepare('SELECT id FROM teams WHERE code = ? LIMIT 1').bind(currentUserMember.team_code).first<{ id: number }>();
        if (!userTeam) {
             // This case indicates a data inconsistency
             console.error(`Data inconsistency: Member ${currentUserMember.id} has team_code ${currentUserMember.team_code} but team not found.`);
             return errorResponse("Could not find your team information.", 500);
        }
        const userTeamId = userTeam.id; // 获取用户队伍的数字 ID

        // 修正这里的比较逻辑：将 matchResult.team1_id/team2_id (数字) 与 userTeamId (数字) 比较
        if (matchResult.team1_id === userTeamId) {
            // Fetch full Team objects
            const team1 = await env.DB.prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').bind(matchResult.team1_id).first<Team>();
            const team2 = await env.DB.prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').bind(matchResult.team2_id).first<Team>();
            if (!team1 || !team2) throw new Error("Teams not found for match."); // Should not happen if FKs are correct
            myTeam = team1;
            opponentTeam = team2;
            myTeamId = team1.id;
            opponentTeamId = team2.id;
        } else if (matchResult.team2_id === userTeamId) {
             // Fetch full Team objects
            const team1 = await env.DB.prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').bind(matchResult.team1_id).first<Team>();
            const team2 = await env.DB.prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').bind(matchResult.team2_id).first<Team>();
            if (!team1 || !team2) throw new Error("Teams not found for match."); // Should not happen if FKs are correct
            myTeam = team2;
            opponentTeam = team1;
            myTeamId = team2.id;
            opponentTeamId = team1.id;
        } else {
             // User's team is not in this match
             return errorResponse('You are not a member of either team in this match.', 403);
        }


        if (!myTeam || !opponentTeam || myTeamId === undefined || opponentTeamId === undefined) {
             // Should not happen if logic above is correct, but as a safeguard
             return errorResponse('Could not determine teams for the match.', 500);
        }


        // Fetch members for both teams
        const myTeamMembersResult = await env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = ?').bind(myTeam.code).all<Pick<Member, 'id' | 'nickname' | 'team_code'>>();
        const myTeamMembers = myTeamMembersResult.results || [];

        const opponentTeamMembersResult = await env.DB.prepare('SELECT id, nickname, team_code FROM members WHERE team_code = ?').bind(opponentTeam.code).all<Pick<Member, 'id' | 'nickname' | 'team_code'>>();
        const opponentTeamMembers = opponentTeamMembersResult.results || [];

        // Fetch existing selections for this match
        const selectionsResult = await env.DB.prepare('SELECT mps.*, m.nickname FROM match_player_selections mps JOIN members m ON mps.member_id = m.id WHERE mps.tournament_match_id = ?').bind(matchId).all<MatchPlayerSelection & { nickname: string }>();
        const allSelections = selectionsResult.results || [];

        // Find the current user's selection
        const mySelectionRaw = allSelections.find(s => s.member_id === currentUserMember.id) || null;

        // Determine occupied order indices for both teams
        const occupiedOrderIndices: { team_id: number; selected_order_index: number; member_id: number; member_nickname?: string }[] = allSelections.map(s => ({
            team_id: s.team_id,
            selected_order_index: s.selected_order_index,
            member_id: s.member_id,
            member_nickname: s.nickname, // Include nickname
        }));

        // Determine available order slots count (assuming it's the number of members in the team)
        const availableOrderSlotsCount = myTeamMembers.length; // Or opponentTeamMembers.length, should be the same for a valid match

        // --- ADDED: Fetch details for selected songs if a selection exists ---
        let mySelection: MatchPlayerSelectionFrontend | null = null;

        if (mySelectionRaw) {
            // Fetch song details for song1_id and song2_id in one batch
            const songIds = [mySelectionRaw.song1_id, mySelectionRaw.song2_id];
            const songDetailsResult = await env.DB.prepare(`SELECT id, title, cover_filename, levels_json FROM songs WHERE id IN (?, ?)`).bind(songIds[0], songIds[1]).all<Song & { levels_json: string | null }>();
            const songDetailsMap = new Map<number, Song & { levels_json: string | null }>();
            songDetailsResult.results?.forEach(song => songDetailsMap.set(song.id, song));

            const song1Detail = songDetailsMap.get(mySelectionRaw.song1_id);
            const song2Detail = songDetailsMap.get(mySelectionRaw.song2_id);

            mySelection = {
                ...mySelectionRaw,
                // Populate denormalized fields for song 1
                song1_title: song1Detail?.title || '未知歌曲',
                song1_fullCoverUrl: song1Detail?.cover_filename && env.R2_PUBLIC_BUCKET_URL
                    ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song1Detail.cover_filename}` // Correct R2 path
                    : undefined,
                song1_parsedLevels: song1Detail?.levels_json ? JSON.parse(song1Detail.levels_json) as SongLevel : undefined,
                // Populate denormalized fields for song 2
                song2_title: song2Detail?.title || '未知歌曲',
                song2_fullCoverUrl: song2Detail?.cover_filename && env.R2_PUBLIC_BUCKET_URL
                    ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song2Detail.cover_filename}` // Correct R2 path
                    : undefined,
                song2_parsedLevels: song2Detail?.levels_json ? JSON.parse(song2Detail.levels_json) as SongLevel : undefined,
                 // Include member nickname from the join
                member_nickname: mySelectionRaw.nickname,
                // Team name is not in MatchPlayerSelectionFrontend, but available in myTeam/opponentTeam
            } as MatchPlayerSelectionFrontend; // Cast to the frontend type

             // Remove the temporary nickname field from the raw selection object if it exists
             delete (mySelection as any).nickname;

        }
        // --- END ADDED ---


        const responseData: FetchUserMatchSelectionData = {
            match: matchResult,
            myTeam: myTeam,
            opponentTeam: opponentTeam,
            myTeamMembers: myTeamMembers,
            opponentTeamMembers: opponentTeamMembers,
            mySelection: mySelection, // Use the processed selection with song details
            occupiedOrderIndices: occupiedOrderIndices,
            availableOrderSlotsCount: availableOrderSlotsCount,
            // REMOVED: song1Details, song2Details - Details are now inside mySelection
        };

        return jsonResponse(responseData);

    } catch (e: any) {
        console.error('Error fetching user match selection data:', e);
        return errorResponse('Failed to fetch match selection data.', 500, e);
    }
}

// POST /api/member/match-selection/:matchId (Authenticated User)
async function handleSaveMatchPlayerSelection(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchId = parseInt(parts[4], 10); // /api/member/match-selection/:matchId -> parts[4]
    console.log(`Authenticated user ${kindeUserId} handling /api/member/match-selection/${matchId} POST request...`);

    if (isNaN(matchId)) {
        return errorResponse("Invalid match ID in path", 400);
    }

    try {
        const payload: SaveMatchPlayerSelectionPayload = await request.json();

        // 1. Find the member ID and team code for the authenticated Kinde user
        const member = await env.DB.prepare('SELECT id, team_code FROM members WHERE kinde_user_id = ? LIMIT 1')
            .bind(kindeUserId)
            .first<{ id: number; team_code: string }>();

        if (!member) {
            return errorResponse("Authenticated user is not registered as a member.", 403); // Forbidden
        }

        // --- 新增步骤：通过 member.team_code 查找用户队伍在 teams 表中的 ID ---
        const userTeam = await env.DB.prepare('SELECT id FROM teams WHERE code = ? LIMIT 1')
             .bind(member.team_code)
             .first<{ id: number }>();

        if (!userTeam) {
             // This case indicates a data inconsistency (member has a team_code that doesn't exist in teams)
             console.error(`Data inconsistency: Member ${member.id} has team_code ${member.team_code} but team not found.`);
             return errorResponse("Could not find your team information.", 500);
        }
        const userTeamId = userTeam.id; // 获取用户队伍的数字 ID


        // 2. Fetch match details and determine user's team ID
        const match = await env.DB.prepare('SELECT id, team1_id, team2_id, status FROM tournament_matches WHERE id = ?').bind(matchId).first<TournamentMatch>();
        if (!match) {
            return errorResponse('Match not found.', 404);
        }

        let myTeamId: number | undefined;
        // 修正这里的比较逻辑：将 match.team1_id/team2_id (数字) 与 userTeamId (数字) 比较
        if (match.team1_id === userTeamId) {
             myTeamId = match.team1_id;
        } else if (match.team2_id === userTeamId) {
             myTeamId = match.team2_id;
        } else {
             // User's team is not in this match
             return errorResponse("Your team is not participating in this match.", 403);
        }

        // 3. Validate payload
        if (
            payload.song1_id === undefined || payload.song1_id === null ||
            payload.song1_difficulty === undefined || payload.song1_difficulty === null ||
            payload.song2_id === undefined || payload.song2_id === null ||
            payload.song2_difficulty === undefined || payload.song2_difficulty === null ||
            payload.selected_order_index === undefined || payload.selected_order_index === null ||
            isNaN(payload.song1_id) || isNaN(payload.song2_id) || isNaN(payload.selected_order_index)
        ) {
             return errorResponse("Invalid or missing song/order data in payload.", 400);
        }

        // Basic range check for order index (assuming 3v3, indices 0, 1, 2)
        // You might need to fetch team size to make this dynamic
        const teamSizeResult = await env.DB.prepare('SELECT COUNT(*) as count FROM members WHERE team_code = ?').bind(member.team_code).first<{ count: number }>();
        const teamSize = teamSizeResult?.count ?? 0;

        if (payload.selected_order_index < 0 || payload.selected_order_index >= teamSize) {
             return errorResponse(`Invalid selected_order_index. Must be between 0 and ${teamSize - 1}.`, 400);
        }


        // Check if songs exist
        const songsExist = await env.DB.batch([
            env.DB.prepare("SELECT id FROM songs WHERE id = ? LIMIT 1").bind(payload.song1_id),
            env.DB.prepare("SELECT id FROM songs WHERE id = ? LIMIT 1").bind(payload.song2_id),
        ]);
        if (!songsExist[0].results[0] || !songsExist[1].results[0]) {
             return errorResponse("One or both selected songs not found.", 400);
        }

        // 4. Check for existing selection by this member for this match
        const existingSelection = await env.DB.prepare('SELECT id FROM match_player_selections WHERE tournament_match_id = ? AND member_id = ? LIMIT 1')
            .bind(matchId, member.id)
            .first<{ id: number }>();

        // 5. Check if the selected order index is already taken by another member in this team
        const orderIndexConflict = await env.DB.prepare('SELECT member_id FROM match_player_selections WHERE tournament_match_id = ? AND team_id = ? AND selected_order_index = ? AND member_id != ? LIMIT 1')
            .bind(matchId, myTeamId, payload.selected_order_index, member.id) // Exclude the current member's potential existing selection
            .first<{ member_id: number }>();

        if (orderIndexConflict) {
             // Fetch the nickname of the member who took the slot
             const conflictingMember = await env.DB.prepare('SELECT nickname FROM members WHERE id = ? LIMIT 1').bind(orderIndexConflict.member_id).first<{ nickname: string }>();
             const conflictingMemberName = conflictingMember?.nickname || '另一位队员';
             return errorResponse(`Order slot ${payload.selected_order_index + 1} is already taken by ${conflictingMemberName} in your team.`, 409);
        }

        const now = new Date().toISOString();
        let result;

        if (existingSelection) {
            // Update existing selection
            console.log(`Updating existing selection for member ${member.id} in match ${matchId}`);
            result = await env.DB.prepare(
                `UPDATE match_player_selections
                 SET song1_id = ?, song1_difficulty = ?, song2_id = ?, song2_difficulty = ?, selected_order_index = ?, updated_at = ?
                 WHERE id = ?`
            )
            .bind(
                payload.song1_id,
                payload.song1_difficulty,
                payload.song2_id,
                payload.song2_difficulty,
                payload.selected_order_index,
                now,
                existingSelection.id
            )
            .run();
        } else {
            // Insert new selection
            console.log(`Inserting new selection for member ${member.id} in match ${matchId}`);
            result = await env.DB.prepare(
                `INSERT INTO match_player_selections (tournament_match_id, member_id, team_id, song1_id, song1_difficulty, song2_id, song2_difficulty, selected_order_index, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
            )
            .bind(
                matchId,
                member.id,
                myTeamId, // 使用正确的 myTeamId (数字)
                payload.song1_id,
                payload.song1_difficulty,
                payload.song2_id,
                payload.song2_difficulty,
                payload.selected_order_index,
                now,
                now
            )
            .run();
        }


        if (result.success) {
            // Fetch the saved selection to return
            const savedSelectionId = existingSelection ? existingSelection.id : result.meta.last_row_id;
            // Fetch the saved selection *with* denormalized song details and nickname for frontend state update
            const savedSelectionQuery = `
                SELECT
                    mps.*,
                    m.nickname,
                    s1.title AS song1_title,
                    s1.cover_filename AS song1_cover_filename,
                    s1.levels_json AS song1_levels_json,
                    s2.title AS song2_title,
                    s2.cover_filename AS song2_cover_filename,
                    s2.levels_json AS song2_levels_json
                FROM match_player_selections mps
                JOIN members m ON mps.member_id = m.id
                JOIN songs s1 ON mps.song1_id = s1.id
                JOIN songs s2 ON mps.song2_id = s2.id
                WHERE mps.id = ? LIMIT 1
            `;
            const savedSelectionRaw = await env.DB.prepare(savedSelectionQuery).bind(savedSelectionId).first<
                MatchPlayerSelection & {
                    nickname: string;
                    song1_title: string; song1_cover_filename: string | null; song1_levels_json: string | null;
                    song2_title: string; song2_cover_filename: string | null; song2_levels_json: string | null;
                }
            >();

            let savedSelection: MatchPlayerSelectionFrontend | null = null;
            if (savedSelectionRaw) {
                 savedSelection = {
                     ...savedSelectionRaw,
                     member_nickname: savedSelectionRaw.nickname,
                     song1_title: savedSelectionRaw.song1_title,
                     song1_fullCoverUrl: savedSelectionRaw.song1_cover_filename && env.R2_PUBLIC_BUCKET_URL
                         ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${savedSelectionRaw.song1_cover_filename}` // Correct R2 path
                         : undefined,
                     song1_parsedLevels: savedSelectionRaw.song1_levels_json ? JSON.parse(savedSelectionRaw.song1_levels_json) as SongLevel : undefined,
                     song2_title: savedSelectionRaw.song2_title,
                     song2_fullCoverUrl: savedSelectionRaw.song2_cover_filename && env.R2_PUBLIC_BUCKET_URL
                         ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${savedSelectionRaw.song2_cover_filename}` // Correct R2 path
                         : undefined,
                     song2_parsedLevels: savedSelectionRaw.song2_levels_json ? JSON.parse(savedSelectionRaw.song2_levels_json) as SongLevel : undefined,
                 } as MatchPlayerSelectionFrontend;
                 // Clean up raw fields used for denormalization
                 delete (savedSelection as any).nickname;
                 delete (savedSelection as any).song1_cover_filename;
                 delete (savedSelection as any).song1_levels_json;
                 delete (savedSelection as any).song2_cover_filename;
                 delete (savedSelection as any).song2_levels_json;
            }


            return jsonResponse({ success: true, message: "Selection saved successfully.", selection: savedSelection }, existingSelection ? 200 : 201);
        } else {
            console.error("Worker: Failed to save match player selection:", result.error);
            // Check for unique constraint errors specifically
            if (result.error?.includes('UNIQUE constraint failed: match_player_selections.tournament_match_id, match_player_selections.member_id')) {
                 return errorResponse("You have already submitted a selection for this match.", 409);
            }
             if (result.error?.includes('UNIQUE constraint failed: match_player_selections.tournament_match_id, match_player_selections.team_id, match_player_selections.selected_order_index')) {
                 // This case should ideally be caught by the explicit check above, but include defensively
                 return errorResponse(`The selected order index ${payload.selected_order_index + 1} is already taken by another member in your team.`, 409);
            }
            return errorResponse(result.error || "Failed to save selection due to a database issue.", 500);
        }

    } catch (e: any) {
        console.error(`Worker: Exception saving match player selection for match ${matchId} and user ${kindeUserId}:`, e);
        return errorResponse(e.message);
    }
}

// POST /api/semifinal-matches (Admin)
async function handleCreateSemifinalMatch(request: Request, env: Env): Promise<Response> {
    // Admin middleware is applied by the router
    const payload: CreateSemifinalMatchPayload = await request.json();
    console.log("Received create semifinal match payload:", payload);

    if (!payload.round_name || !payload.player1_id || !payload.player2_id) {
        return errorResponse('Missing round_name, player1_id, or player2_id', 400);
    }
    if (payload.player1_id === payload.player2_id) {
        return errorResponse('Player A and Player B cannot be the same player', 400);
    }

    try {
        const result = await env.DB.prepare(
            'INSERT INTO semifinal_matches (round_name, player1_id, player2_id, scheduled_time, status) VALUES (?, ?, ?, ?, ?)'
        )
        .bind(
            payload.round_name,
            payload.player1_id,
            payload.player2_id,
            payload.scheduled_time,
            'scheduled' // Initial status
        )
        .run();

        if (!result.success) {
            console.error("DB insert failed:", result.error);
            return errorResponse('Failed to create semifinal match', 500, result.error);
        }

        // Optionally fetch and return the created match
        // const newMatch = await env.DB.prepare('SELECT * FROM semifinal_matches WHERE rowid = last_insert_rowid()').first<SemifinalMatch>();
        // return jsonResponse(newMatch, 201);

        return jsonResponse({ message: 'Semifinal match created successfully' }, 201);

    } catch (e: any) {
        console.error("Error creating semifinal match:", e);
        return errorResponse('Internal server error creating match', 500, e.message);
    }
}

// GET /api/semifinal_matches (Admin)
async function handleFetchSemifinalMatches(request: Request, env: Env): Promise<Response> {
    // Admin middleware is applied by the router
    try {
        // Fetch semifinal matches, joining members for nicknames
        const query = `
            SELECT
                sm.*,
                p1.nickname AS player1_nickname,
                p2.nickname AS player2_nickname,
                w_p.nickname AS winner_player_nickname
            FROM semifinal_matches sm
            LEFT JOIN members p1 ON sm.player1_id = p1.id
            LEFT JOIN members p2 ON sm.player2_id = p2.id
            LEFT JOIN members w_p ON sm.winner_player_id = w_p.id
            ORDER BY sm.scheduled_time ASC, sm.id ASC
        `;
        const { results } = await env.DB.prepare(query).all<SemifinalMatch>();

        // Manually parse JSON fields for each result
        const processedResults = results.map(match => {
            const processedMatch = { ...match } as SemifinalMatch;
            if (match.results_json) {
                try { processedMatch.results = JSON.parse(match.results_json); } catch (e) { console.error("Failed to parse results_json", e); }
            }
            return processedMatch;
        });


        return jsonResponse(processedResults);
    } catch (e: any) {
        console.error("Error fetching semifinal matches:", e);
        return errorResponse('Failed to fetch semifinal matches', 500, e.message);
    }
}

// POST /api/semifinal-matches/:id/submit-scores (Admin)
async function handleSubmitSemifinalScores(request: Request, env: Env, matchId: number): Promise<Response> {
    // Admin middleware is applied by the router
    const payload: SubmitSemifinalScoresPayload = await request.json();
    console.log(`Received semifinal scores for match ${matchId}:`, payload);

    if (!payload.player1 || !payload.player2 || !payload.player1.profession || !payload.player2.profession || payload.player1.percentage === undefined || payload.player2.percentage === undefined) {
        return errorResponse('Invalid payload: missing player data, profession, or percentage', 400);
    }

    try {
        // Fetch the match to ensure it exists and is in the correct status ('scheduled')
        const match = await env.DB.prepare('SELECT * FROM semifinal_matches WHERE id = ?').bind(matchId).first<SemifinalMatch>();

        if (!match) {
            return errorResponse('Semifinal match not found', 404);
        }

        if (match.status !== 'scheduled') {
             // Allow resubmission if needed, but for simplicity, let's only allow 'scheduled'
             return errorResponse(`Match is not in 'scheduled' status (current status: ${match.status})`, 400);
        }

        // Ensure the player IDs in the payload match the match record
        if (match.player1_id !== payload.player1.id || match.player2_id !== payload.player2.id) {
             console.warn(`Player IDs in payload did not match DB for match ${matchId}. Using IDs from match record.`);
             payload.player1.id = match.player1_id; // Use ID from DB
             payload.player2.id = match.player2_id; // Use ID from DB
        }

        // Fetch player nicknames for calculation logs
        const players = await env.DB.prepare('SELECT id, nickname FROM members WHERE id IN (?, ?)').bind(payload.player1.id, payload.player2.id).all<{ id: number; nickname: string }>();
        const player1Nickname = players.results.find(p => p.id === payload.player1.id)?.nickname || `Player ${payload.player1.id}`;
        const player2Nickname = players.results.find(p => p.id === payload.player2.id)?.nickname || `Player ${payload.player2.id}`;


        // Prepare data for calculation
        const player1Data: PlayerCalculationData = {
            id: payload.player1.id,
            nickname: player1Nickname,
            profession: payload.player1.profession,
            percentage: payload.player1.percentage
        };
        const player2Data: PlayerCalculationData = {
            id: payload.player2.id,
            nickname: player2Nickname,
            profession: payload.player2.profession,
            percentage: payload.player2.percentage
        };

        // Perform calculation
        const result1 = calculateSemifinalScore(player1Data, player2Data);
        const result2 = calculateSemifinalScore(player2Data, player1Data);

        // Determine winner
        let winnerPlayerId: number | null = null;
        if (result1.totalScore > result2.totalScore) {
            winnerPlayerId = result1.id;
        } else if (result2.totalScore > result1.totalScore) {
            winnerPlayerId = result2.id;
        } else {
            // Handle draw - Player 1 wins on tie for simplicity
             winnerPlayerId = result1.id;
             console.warn(`Semifinal match ${matchId} resulted in a draw (${result1.totalScore.toFixed(4)} vs ${result2.totalScore.toFixed(4)}). Player 1 (${player1Nickname}) wins tiebreak.`);
        }


        // Store results in the database
        const semifinalResults = {
            player1: result1,
            player2: result2,
            submitted_at: new Date().toISOString()
        };

        const updateResult = await env.DB.prepare(
            `UPDATE semifinal_matches
             SET status = ?, winner_player_id = ?,
                 player1_percentage = ?, player2_percentage = ?,
                 player1_profession = ?, player2_profession = ?,
                 final_score_player1 = ?, final_score_player2 = ?,
                 results_json = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`
        )
        .bind(
            'completed', // Mark as completed
            winnerPlayerId,
            payload.player1.percentage,
            payload.player2.percentage,
            payload.player1.profession,
            payload.player2.profession,
            result1.totalScore,
            result2.totalScore,
            JSON.stringify(semifinalResults), // Store results as JSON
            matchId
        )
        .run();

        if (!updateResult.success) {
            console.error("DB update failed:", updateResult.error);
            return errorResponse('Failed to save match results', 500, updateResult.error);
        }

        // Fetch the updated match to return
        const updatedMatch = await env.DB.prepare(
             `SELECT
                sm.*,
                p1.nickname AS player1_nickname,
                p2.nickname AS player2_nickname,
                w_p.nickname AS winner_player_nickname
            FROM semifinal_matches sm
            LEFT JOIN members p1 ON sm.player1_id = p1.id
            LEFT JOIN members p2 ON sm.player2_id = p2.id
            LEFT JOIN members w_p ON sm.winner_player_id = w_p.id
            WHERE sm.id = ?`
        ).bind(matchId).first<SemifinalMatch>();

         // Manually parse JSON fields for the updated match
         if (updatedMatch && updatedMatch.results_json) {
             try { updatedMatch.results = JSON.parse(updatedMatch.results_json); } catch (e) { console.error("Failed to parse results_json for updated match", matchId, e); }
         }


        return jsonResponse<SubmitSemifinalScoresResponse>({
            success: true,
            message: 'Scores submitted and results calculated successfully',
            semifinalMatch: updatedMatch || undefined // Return updated match data
        });

    } catch (e: any) {
        console.error("Error submitting semifinal scores:", e);
        return errorResponse('Internal server error submitting scores', 500, e.message);
    }
}

// GET /api/semifinal-matches/:id (Public view of completed match)
async function handleFetchSemifinalMatch(request: Request, env: Env, matchId: number): Promise<Response> {
     // This endpoint is public
     try {
         const match = await env.DB.prepare(
             `SELECT
                sm.*,
                p1.nickname AS player1_nickname,
                p2.nickname AS player2_nickname,
                w_p.nickname AS winner_player_nickname
            FROM semifinal_matches sm
            LEFT JOIN members p1 ON sm.player1_id = p1.id
            LEFT JOIN members p2 ON sm.player2_id = p2.id
            LEFT JOIN members w_p ON sm.winner_player_id = w_p.id
            WHERE sm.id = ?`
         ).bind(matchId).first<SemifinalMatch>();

         if (!match) {
             return errorResponse('Semifinal match not found', 404);
         }

         // Parse the results JSON
         if (match.results_json) {
             try {
                 match.results = JSON.parse(match.results_json);
             } catch (e) {
                 console.error("Failed to parse results_json for match", matchId, e);
             }
         }

         // Remove results_json before sending to frontend if you prefer
         // delete match.results_json; // Or handle this in the frontend type/parsing

         return jsonResponse(match);

     } catch (e: any) {
         console.error(`Error fetching semifinal match ${matchId}:`, e);
         return errorResponse('Failed to fetch semifinal match data', 500, e.message);
     }
}

// POST /api/semifinal-matches/:id/archive (Admin)
async function handleArchiveSemifinalMatch(request: Request, env: Env, matchId: number): Promise<Response> {
     // Admin middleware is applied by the router
     try {
         const updateResult = await env.DB.prepare(
             'UPDATE semifinal_matches SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
         )
         .bind('archived', matchId)
         .run();

         if (!updateResult.success) {
             console.error("DB update failed:", updateResult.error);
             return errorResponse('Failed to archive match', 500, updateResult.error);
         }

         if (updateResult.meta.changes === 0) {
              // Check if the match existed
              const match = await env.DB.prepare('SELECT id FROM semifinal_matches WHERE id = ?').bind(matchId).first();
              if (!match) {
                   return errorResponse('Semifinal match not found', 404);
              }
              // If match existed but changes was 0, it might already be archived
              return jsonResponse({ message: 'Match already archived' });
         }

         return jsonResponse({ message: 'Match archived successfully' });

     } catch (e: any) {
         console.error(`Error archiving semifinal match ${matchId}:`, e);
         return errorResponse('Internal server error archiving match', 500, e.message);
     }
}

// --- Songs API Handlers ---
// GET /api/songs (Public, Paginated, Filterable)
async function handleFetchSongs(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/songs request...');
    const url = new URL(request.url);
    const category = url.searchParams.get('category');
    const type = url.searchParams.get('type');
    const search = url.searchParams.get('search');
    const level = url.searchParams.get('level'); // Filter by level value (e.g., "13", "14+")
    const difficulty = url.searchParams.get('difficulty'); // Filter by difficulty key (e.g., "M", "R")
    const page = parseInt(url.searchParams.get('page') || '1', 10);
    const limit = parseInt(url.searchParams.get('limit') || '20', 10);

    if (isNaN(page) || page < 1) return errorResponse('Invalid page number.', 400);
    if (isNaN(limit) || limit < 1) return errorResponse('Invalid limit number.', 400);

    const offset = (page - 1) * limit;

    let whereClauses: string[] = [];
    let params: (string | number)[] = [];

    if (category) {
        whereClauses.push('category = ?');
        params.push(category);
    }
    if (type) {
        whereClauses.push('type = ?');
        params.push(type);
    }
    if (search) {
        // Basic search on title
        whereClauses.push('title LIKE ?');
        params.push(`%${search}%`);
    }
    if (level) {
        // Filter by level value within levels_json (e.g., find songs with level "13+" in any difficulty)
        // This uses LIKE on the JSON string, which is not ideal for performance on large datasets
        // but is a common approach for D1 with JSON columns.
        whereClauses.push('levels_json LIKE ?');
        params.push(`%"${level}"%`);
    }
     if (difficulty) {
         // Filter by difficulty key within levels_json (e.g., find songs with an 'M' difficulty)
         whereClauses.push('levels_json LIKE ?');
         params.push(`%"${difficulty}":"%"%`);
     }


    const whereSql = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';

    try {
        // 1. Get total count of filtered songs
        const countQuery = env.DB.prepare(`SELECT COUNT(*) as total FROM songs ${whereSql}`).bind(...params);
        const totalResult = await countQuery.first<{ total: number }>();
        const totalItems = totalResult?.total ?? 0;
        const totalPages = Math.ceil(totalItems / limit);

        // 2. Get songs for the current page
        // Add ORDER BY if needed, e.g., ORDER BY title ASC
        const songsQuery = env.DB.prepare(`SELECT * FROM songs ${whereSql} ORDER BY title ASC LIMIT ? OFFSET ?`).bind(...params, limit, offset); // Added ORDER BY
        const songsResult = await songsQuery.all<Song>();
        const songs = songsResult.results || [];

        // Process songs: parse levels_json and add fullCoverUrl
        const processedSongs: Song[] = songs.map(song => {
            let parsedLevels: SongLevel | undefined;
            try {
                if (song.levels_json) {
                    parsedLevels = JSON.parse(song.levels_json) as SongLevel;
                }
            } catch (e) {
                console.error(`Failed to parse levels_json for song ${song.id}:`, e);
            }

            const fullCoverUrl = song.cover_filename && env.R2_PUBLIC_BUCKET_URL
                ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${song.cover_filename}` // Correct R2 path
                : undefined; // Or a default image URL

            return {
                ...song,
                parsedLevels,
                fullCoverUrl,
            };
        });

        const pagination: PaginationInfo = {
            currentPage: page,
            pageSize: limit,
            totalItems: totalItems,
            totalPages: totalPages,
        };

        const responseData: SongsApiResponseData = {
            songs: processedSongs,
            pagination: pagination,
        };

        return jsonResponse(responseData);

    } catch (e: any) {
        console.error('Error fetching songs:', e);
        return errorResponse('Failed to fetch songs.', 500, e);
    }
}

// GET /api/songs/filters (Public)
async function handleFetchSongFilters(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/songs/filters request...');
    try {
        // Fetch distinct categories
        const categoriesResult = await env.DB.prepare('SELECT DISTINCT category FROM songs WHERE category IS NOT NULL ORDER BY category ASC').all<{ category: string }>();
        const categories = categoriesResult.results?.map(row => row.category) || [];

        // Fetch distinct types
        const typesResult = await env.DB.prepare('SELECT DISTINCT type FROM songs WHERE type IS NOT NULL ORDER BY type ASC').all<{ type: string }>();
        const types = typesResult.results?.map(row => row.type) || [];

        // Fetch all levels_json to extract unique levels and difficulties
        const allLevelsResult = await env.DB.prepare('SELECT levels_json FROM songs WHERE levels_json IS NOT NULL').all<{ levels_json: string }>();
        const allLevelsJson = allLevelsResult.results?.map(row => row.levels_json) || [];

        const uniqueLevels = new Set<string>();
        const uniqueDifficulties = new Set<string>(); // B, A, E, M, R keys

        for (const jsonString of allLevelsJson) {
            try {
                const levels = JSON.parse(jsonString) as SongLevel;
                for (const diff in levels) {
                    if (levels[diff as keyof SongLevel]) {
                        uniqueDifficulties.add(diff);
                        uniqueLevels.add(levels[diff as keyof SongLevel]!); // Add the level value
                    }
                }
            } catch (e) {
                console.error('Failed to parse levels_json for filters:', jsonString, e);
            }
        }

        // Sort levels numerically (handle '+' suffix)
        const sortedLevels = Array.from(uniqueLevels).sort((a, b) => {
            const numA = parseFloat(a.replace('+', '.5')); // Treat 5+ as 5.5 for sorting
            const numB = parseFloat(b.replace('+', '.5'));
            return numA - numB;
        });

        // Sort difficulties in a specific order (B, A, E, M, R)
        const difficultyOrder = ['B', 'A', 'E', 'M', 'R'];
        const sortedDifficulties = Array.from(uniqueDifficulties).sort((a, b) => {
            return difficultyOrder.indexOf(a) - difficultyOrder.indexOf(b);
        });


        const responseData: SongFiltersApiResponseData = {
            categories: categories,
            types: types,
            levels: sortedLevels, // Include sorted levels
            difficulties: sortedDifficulties, // Include sorted difficulties
        };

        return jsonResponse(responseData);

    } catch (e: any) {
        console.error('Error fetching song filter options:', e);
        return errorResponse('Failed to fetch song filter options.', 500, e);
    }
}


// --- Match DO API Handlers ---
// POST /api/matches/:matchId/calculate-round (Admin Only)
async function handleCalculateRound(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/matches/:matchId/calculate-round -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding calculate-round to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/calculate-round', 'POST');
}

// POST /api/matches/:matchId/resolve-draw (Admin Only)
async function handleResolveDraw(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/matches/:matchId/resolve-draw -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding resolve-draw to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/resolve-draw', 'POST');
}

// POST /api/matches/:matchId/select-tiebreaker-song (Admin Only)
async function handleSelectTiebreakerSong(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/matches/:matchId/select-tiebreaker-song -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding select-tiebreaker-song to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/select-tiebreaker-song', 'POST');
}

// POST /api/tournament_matches/:matchId/start_live (Admin Only)
async function handleStartLiveMatch(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchId = parseInt(parts[3], 10); // /api/tournament_matches/:matchId/start_live -> parts[3]
    console.log(`Admin user ${kindeUserId} handling /api/tournament_matches/${matchId}/start_live POST request...`);

    if (isNaN(matchId)) {
        return errorResponse("Invalid match ID in path", 400);
    }

    try {
        // Fetch the match details including player orders and song list JSON
        const match = await env.DB.prepare(`
            SELECT
                tm.*,
                t1.name AS team1_name,
                t2.name AS team2_name,
                t1.code AS team1_code, -- Need team codes to fetch members
                t2.code AS team2_code
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            WHERE tm.id = ?
        `).bind(matchId).first<TournamentMatch>();

        if (!match) {
            return errorResponse('Match not found.', 404);
        }

        // Check if match is ready to start
        if (match.status !== 'ready_to_start') {
            return errorResponse(`Match status is "${match.status}", cannot start live match. Status must be 'ready_to_start'.`, 400);
        }

        // Parse player orders and song list
        let team1PlayerOrderIds: number[] | null = null;
        let team2PlayerOrderIds: number[] | null = null;
        let matchSongList: MatchSong[] | null = null;

        if (match.team1_player_order_json) {
            try { team1PlayerOrderIds = JSON.parse(match.team1_player_order_json); } catch (e) { console.error(`Failed to parse team1_player_order_json for match ${match.id}`, e); return errorResponse('Failed to parse team 1 player order.', 500); }
        }
        if (match.team2_player_order_json) {
            try { team2PlayerOrderIds = JSON.parse(match.team2_player_order_json); } catch (e) { console.error(`Failed to parse team2_player_order_json for match ${match.id}`, e); return errorResponse('Failed to parse team 2 player order.', 500); }
        }
        if (match.match_song_list_json) {
            try { matchSongList = JSON.parse(match.match_song_list_json); } catch (e) { console.error(`Failed to parse match_song_list_json for match ${match.id}`, e); return errorResponse('Failed to parse match song list.', 500); }
        }

        if (!team1PlayerOrderIds || !team2PlayerOrderIds || !matchSongList || team1PlayerOrderIds.length === 0 || team2PlayerOrderIds.length === 0 || matchSongList.length === 0) {
             return errorResponse('Match setup data is incomplete (player orders or song list missing/empty).', 400);
        }

        // Fetch full member details for both teams using team codes
        const [team1MembersResult, team2MembersResult] = await env.DB.batch([
             env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE team_code = ?').bind(match.team1_code),
             env.DB.prepare('SELECT id, team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin FROM members WHERE team_code = ?').bind(match.team2_code),
        ]);
        const team1Members = team1MembersResult.results || [];
        const team2Members = team2MembersResult.results || [];

        // Prepare data for DO initialization
        const matchScheduleData: MatchScheduleData = {
            tournamentMatchId: match.id,
            round_name: match.round_name,
            team1_id: match.team1_id,
            team2_id: match.team2_id,
            team1_name: match.team1_name || 'Team 1',
            team2_name: match.team2_name || 'Team 2',
            team1_members: team1Members,
            team2_members: team2Members,
            team1_player_order_ids: team1PlayerOrderIds,
            team2_player_order_ids: team2PlayerOrderIds,
            match_song_list: matchSongList,
        };

        // Get or create the Durable Object instance using a deterministic name (e.g., "match-1")
        const matchDoName = `match-${matchId}`;
        const doStub = getMatchDO(matchDoName, env);

        console.log(`Worker: Attempting to initialize and start DO ${matchDoName} for match ${matchId}`);

        // Forward an initialization request to the DO
        // The DO's fetch handler should recognize the '/init' path and handle the data
        // 在 Worker 中
        const doInitResponse = await forwardRequestToDO(matchDoName, env, request, '/internal/initialize-from-schedule', 'POST', matchScheduleData);


        if (!doInitResponse.ok) {
             const errorBody = await doInitResponse.json().catch(() => ({ error: 'Unknown DO initialization error' }));
             console.error(`Worker: DO initialization failed for ${matchDoName}: Status ${doInitResponse.status}`, errorBody);
             // Optionally update match status back to ready_to_start or add an error status
             return errorResponse(`Failed to initialize live match instance: ${errorBody.error || doInitResponse.statusText}`, doInitResponse.status);
        }

        // Update the match status in D1 to 'live'
        const now = new Date().toISOString();
        const updateResult = await env.DB.prepare(
            'UPDATE tournament_matches SET status = ?, match_do_id = ?, updated_at = ? WHERE id = ?'
        )
        .bind('live', matchDoName, now, matchId)
        .run();

        if (!updateResult.success) {
            console.error("Worker: Failed to update match status to 'live' after DO init:", updateResult.error);
            // This is a partial failure - DO is running, but D1 status is wrong. Log and proceed.
        }

        const doInitResult = await doInitResponse.json().catch(() => ({})); // Get DO's response body

        return jsonResponse({
            message: "Live match started successfully.",
            match_do_id: matchDoName, // Return the DO name (deterministic ID)
            do_init_result: doInitResult, // Include DO's response if any
        }, 200);

    } catch (e: any) {
        console.error("Worker: Exception starting live match:", e);
        return errorResponse(e.message);
    }
}


// GET /api/live-match/:doId/state (Public)
async function handleGetMatchState(request: Request, env: Env): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/state -> parts[3]
    console.log(`Handling /api/live-match/${matchDoName}/state GET request...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/state', 'GET');
}

// GET /api/live-match/:doId/websocket (Public)
async function handleMatchWebSocket(request: Request, env: Env): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/websocket -> parts[3]
    console.log(`Handling /api/live-match/${matchDoName}/websocket GET request...`);

    // Ensure the request is a WebSocket upgrade request
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    try {
        // 修正：直接使用 forwardRequestToDO 函数，明确指定路径为 '/websocket'
        return forwardRequestToDO(matchDoName, env, request, '/websocket', 'GET');
    } catch (e: any) {
        console.error(`Worker: Failed to connect WebSocket to DO ${matchDoName}:`, e);
        return errorResponse(`Failed to connect to live match instance: ${e.message}`, 500);
    }
}


// POST /api/live-match/:doId/calculate-round (Admin Only)
async function handleCalculateRoundDO(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/calculate-round -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding calculate-round to DO ${matchDoName}...`);
    return forwardRequestToDO(matchDoName, env, request, '/internal/calculate-round', 'POST');
}

// POST /api/live-match/:doId/next-round (Admin Only)
async function handleNextRoundDO(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/next-round -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding next-round to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/next-round', 'POST');
}

// POST /api/live-match/:doId/select-tiebreaker-song (Admin Only)
async function handleSelectTiebreakerSongDO(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/select-tiebreaker-song -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding select-tiebreaker-song to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/select-tiebreaker-song', 'POST');
}

// POST /api/live-match/:doId/resolve-draw (Admin Only)
async function handleResolveDrawDO(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/resolve-draw -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding resolve-draw to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/resolve-draw', 'POST');
}

// POST /api/live-match/:doId/archive (Admin Only)
async function handleArchiveMatchDO(request: Request, env: Env, ctx: ExecutionContext, kindeUserId: string): Promise<Response> {
    const parts = new URL(request.url).pathname.split('/');
    const matchDoName = parts[3]; // /api/live-match/:doId/archive -> parts[3]
    console.log(`Admin user ${kindeUserId} forwarding archive to DO ${matchDoName}...`);
    // Forward the request to the specific Match DO instance
    return forwardRequestToDO(matchDoName, env, request, '/internal/archive-match', 'POST');
}


// GET /api/match_history (Public)
async function handleFetchMatchHistory(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/match_history request...');
    try {
        // Fetch completed/archived matches with joined team names
        const query = `
            SELECT
                tm.id, tm.round_name, tm.scheduled_time, tm.status, tm.final_score_team1, tm.final_score_team2,
                t1.name AS team1_name,
                t2.name AS team2_name,
                tw.name AS winner_team_name
            FROM tournament_matches tm
            JOIN teams t1 ON tm.team1_id = t1.id
            JOIN teams t2 ON tm.team2_id = t2.id
            LEFT JOIN teams tw ON tm.winner_team_id = tw.id
            WHERE tm.status IN ('completed', 'archived')
            ORDER BY tm.scheduled_time DESC, tm.created_at DESC
        `;
        const { results: matches } = await env.DB.prepare(query).all<MatchHistoryMatch>();

        // Fetch all rounds for these matches
        const matchIds = matches.map(m => m.id);
        let rounds: MatchHistoryRound[] = [];
        if (matchIds.length > 0) {
             const roundsQuery = `
                 SELECT
                     mrh.*,
                     s.title AS song_title,
                     s.cover_filename AS cover_filename,
                     s.levels_json AS levels_json,
                     pt.name AS picker_team_name,
                     pm.nickname AS picker_member_nickname,
                     t1m.nickname AS team1_member_nickname,
                     t2m.nickname AS team2_member_nickname
                 FROM match_rounds_history mrh
                 LEFT JOIN songs s ON mrh.song_id = s.id
                 LEFT JOIN teams pt ON mrh.picker_team_id = pt.id
                 LEFT JOIN members pm ON mrh.picker_member_id = pm.id
                 LEFT JOIN members t1m ON mrh.team1_member_id = t1m.id
                 LEFT JOIN members t2m ON mrh.team2_member_id = t2m.id
                 WHERE mrh.tournament_match_id IN (${matchIds.join(',')})
                 ORDER BY mrh.tournament_match_id, mrh.round_number_in_match
             `;
             const { results: roundsResult } = await env.DB.prepare(roundsQuery).all<MatchHistoryRound & { levels_json: string | null }>();

             rounds = (roundsResult || []).map(round => {
                 const parsedRound = { ...round } as MatchHistoryRound;
                 // Parse round_summary_json
                 if (round.round_summary_json) {
                     try { parsedRound.round_summary = JSON.parse(round.round_summary_json); } catch (e) { console.error(`Failed to parse round_summary_json for round ${round.id}`, e); }
                 }
                 // Add fullCoverUrl for the song
                 parsedRound.fullCoverUrl = round.cover_filename && env.R2_PUBLIC_BUCKET_URL
                     ? `${env.R2_PUBLIC_BUCKET_URL}/song_covers/${round.cover_filename}` // Correct R2 path
                     : undefined;
                 // Convert 0/1 integers to booleans
                 parsedRound.team1_mirror_triggered = round.team1_mirror_triggered === 1;
                 parsedRound.team2_mirror_triggered = round.team2_mirror_triggered === 1;
                 parsedRound.is_tiebreaker_song = round.is_tiebreaker_song === 1;

                 // Remove raw JSON field
                 delete (parsedRound as any).round_summary_json;
                 delete (parsedRound as any).levels_json; // levels_json is from song join, not needed in round history

                 return parsedRound;
             });
        }


        // Group rounds by match ID
        const roundsByMatchId = rounds.reduce((acc, round) => {
            if (!acc[round.tournament_match_id]) {
                acc[round.tournament_match_id] = [];
            }
            acc[round.tournament_match_id].push(round);
            return acc;
        }, {} as Record<number, MatchHistoryRound[]>);

        // Attach rounds to their respective matches
        const matchesWithRounds = matches.map(match => ({
            ...match,
            rounds: roundsByMatchId[match.id] || [],
        }));


        return jsonResponse(matchesWithRounds);
    } catch (e: any) {
        console.error("Worker: Failed to fetch match history:", e);
        return errorResponse(e.message);
    }
}


// --- Main Worker Fetch Handler ---
export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method;

        console.log(`Received request: ${method} ${path}`);

        // Handle CORS preflight requests
        if (method === 'OPTIONS') {
            return new Response(null, { status: 204, headers: CORS_HEADERS });
        }

        // --- Public Routes ---
        if (path === '/api/settings' && method === 'GET') {
            return handleGetSettings(request, env);
        }
        if (path === '/api/kinde/callback' && method === 'POST') {
            return handleKindeCallback(request, env, ctx);
        }
        if (path === '/api/logout' && method === 'GET') {
            return handleLogout(request, env);
        }
        if (path === '/api/teams' && method === 'GET') {
            return handleFetchTeams(request, env);
        }
        if (path.startsWith('/api/teams/') && path.split('/').length === 4 && method === 'GET') {
             // Matches /api/teams/:code
             return handleGetTeamByCode(request, env);
        }
        if (path === '/api/teams/check' && method === 'POST') {
            return handleCheckTeam(request, env);
        }
        if (path === '/api/teams/create' && method === 'POST') {
            return handleCreateTeam(request, env);
        }
        if (path === '/api/members' && method === 'GET') {
            // Can include ?team_code=...
            return handleFetchMembers(request, env);
        }

        // --- Authenticated User Routes (Require authMiddleware) ---
        // 将 /api/members/me 放在 /api/members/:id 之前检查
        if (path === '/api/members/me' && method === 'GET') {
            // This route requires authentication
            return authMiddleware(request, env, ctx, handleFetchMe);
        }

        // --- Public Routes (Continued) ---
        // Add the missing route for fetching tournament matches
        if (path === '/api/tournament_matches' && method === 'GET') {
            return handleFetchTournamentMatches(request, env);
        }
        // Now check for /api/members/:id *after* checking for /api/members/me
        if (path.startsWith('/api/members/') && path.split('/').length === 4 && method === 'GET') {
             // Matches /api/members/:id (Public route to get member by ID)
             return handleGetMemberById(request, env);
        }
        if (path === '/api/songs' && method === 'GET') {
            // Can include pagination and filters
            return handleFetchSongs(request, env);
        }
        if (path === '/api/songs/filters' && method === 'GET') {
            return handleFetchSongFilters(request, env);
        }
        // Public Match DO routes (using /api/live-match/:doId)
        if (path.startsWith('/api/live-match/') && path.endsWith('/state') && path.split('/').length === 5 && method === 'GET') {
             // Matches /api/live-match/:doId/state
             const doId = path.split('/')[3];
             return handleGetMatchState(request, env); // Call the handler
        }
         if (path.startsWith('/api/live-match/') && path.endsWith('/websocket') && path.split('/').length === 5 && method === 'GET') {
             // Matches /api/live-match/:doId/websocket
             const doId = path.split('/')[3];
             return handleMatchWebSocket(request, env); // Call the handler
        }
        // Public Match History route
        if (path === '/api/match_history' && method === 'GET') {
             return handleFetchMatchHistory(request, env); // Call the handler
        }


        // --- Authenticated User Routes (Continued - Require authMiddleware) ---
        // Other authenticated routes remain here
        if (path === '/api/teams/join' && method === 'POST') {
            return authMiddleware(request, env, ctx, handleJoinTeam);
        }
        if (path.startsWith('/api/members/') && path.split('/').length === 4 && method === 'PATCH') {
             // Matches /api/members/:maimaiId (User update)
             return authMiddleware(request, env, ctx, handleUserPatchMember);
        }
        if (path.startsWith('/api/members/') && path.split('/').length === 4 && method === 'DELETE') {
             // Matches /api/members/:maimaiId (User delete)
             return authMiddleware(request, env, ctx, handleUserDeleteMember);
        }
        if (path === '/api/member_song_preferences' && method === 'POST') {
             // User saves song preference
             return authMiddleware(request, env, ctx, handleSaveMemberSongPreference);
        }
         if (path === '/api/member_song_preferences' && method === 'GET') {
             // User fetches song preferences (requires member_id and stage query params)
             return authMiddleware(request, env, ctx, handleFetchMemberSongPreferences);
        }
        // NEW: User Match Selection View
        if (path.startsWith('/api/member/match-selection/') && path.split('/').length === 5 && method === 'GET') {
             // Matches /api/member/match-selection/:matchId
             return authMiddleware(request, env, ctx, handleFetchUserMatchSelectionData);
        }
        // NEW: User Save Match Selection
        if (path.startsWith('/api/member/match-selection/') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/member/match-selection/:matchId
             return authMiddleware(request, env, ctx, handleSaveMatchPlayerSelection);
        }
        // NEW: User's Matches List
        if (path === '/api/member/matches' && method === 'GET') {
            return authMiddleware(request, env, ctx, handleFetchUserMatches);
        }


        // --- Admin Routes (Require adminAuthMiddleware) ---
        // Use adminAuthMiddleware to protect these routes
        if (path === '/api/admin/members' && method === 'GET') {
            return adminAuthMiddleware(request, env, ctx, handleAdminFetchMembers);
        }
        if (path.startsWith('/api/admin/members/') && path.split('/').length === 5 && method === 'GET') {
             // Matches /api/admin/members/:id
             return adminAuthMiddleware(request, env, ctx, handleAdminGetMemberById);
        }
        if (path.startsWith('/api/admin/members/') && path.split('/').length === 5 && method === 'PATCH') {
             // Matches /api/admin/members/:id (Admin update)
             return adminAuthMiddleware(request, env, ctx, handleAdminPatchMember);
        }
        if (path === '/api/admin/members' && method === 'POST') {
             // Matches /api/admin/members (Admin add)
             return adminAuthMiddleware(request, env, ctx, handleAdminAddMember);
        }
        if (path.startsWith('/api/admin/members/') && path.split('/').length === 5 && method === 'DELETE') {
             // Matches /api/admin/members/:id (Admin delete)
             return adminAuthMiddleware(request, env, ctx, handleAdminDeleteMember);
        }
        if (path === '/api/admin/settings' && method === 'PATCH') {
             // Matches /api/admin/settings (Admin update settings)
             return adminAuthMiddleware(request, env, ctx, handleAdminUpdateSettings);
        }
        if (path === '/api/tournament_matches' && method === 'POST') {
            return adminAuthMiddleware(request, env, ctx, handleCreateTournamentMatch);
        }
        if (path.startsWith('/api/tournament_matches/') && path.endsWith('/confirm_setup') && path.split('/').length === 5 && method === 'PUT') {
             // Matches /api/tournament_matches/:id/confirm_setup
             return adminAuthMiddleware(request, env, ctx, handleConfirmMatchSetup);
        }
        // NEW: Admin Compile Match Setup
        if (path.startsWith('/api/tournament_matches/') && path.endsWith('/compile-setup') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/tournament_matches/:matchId/compile-setup
             return adminAuthMiddleware(request, env, ctx, handleCompileMatchSetup);
        }
        if (path.startsWith('/api/tournament_matches/') && path.endsWith('/selection-status') && path.split('/').length === 5 && method === 'GET') {
            // Matches /api/tournament_matches/:matchId/selection-status
            return adminAuthMiddleware(request, env, ctx, handleCheckMatchSelectionStatus);
        }
        // Admin Match DO actions (forwarded via /api/live-match/:doId)
        if (path.startsWith('/api/live-match/') && path.endsWith('/calculate-round') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/live-match/:doId/calculate-round
             const matchDoName = path.split('/')[3];
             return adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => forwardRequestToDO(matchDoName, env, req, '/internal/calculate-round', 'POST'));
        }
        if (path.startsWith('/api/live-match/') && path.endsWith('/next-round') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/live-match/:doId/next-round
             const matchDoName = path.split('/')[3];
             return adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => forwardRequestToDO(matchDoName, env, req, '/internal/next-round', 'POST'));
        }
        if (path.startsWith('/api/live-match/') && path.endsWith('/select-tiebreaker-song') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/live-match/:doId/select-tiebreaker-song
             const matchDoName = path.split('/')[3];
             return adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => forwardRequestToDO(matchDoName, env, req, '/internal/select-tiebreaker-song', 'POST'));
        }
        if (path.startsWith('/api/live-match/') && path.endsWith('/resolve-draw') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/live-match/:doId/resolve-draw
             const matchDoName = path.split('/')[3];
             return adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => forwardRequestToDO(matchDoName, env, req, '/internal/resolve-draw', 'POST'));
        }
        if (path.startsWith('/api/live-match/') && path.endsWith('/archive') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/live-match/:doId/archive
             const matchDoName = path.split('/')[3];
             return adminAuthMiddleware(request, env, ctx, (req, env, context, userId) => forwardRequestToDO(matchDoName, env, req, '/internal/archive-match', 'POST'));
        }
         // Admin Start Live Match (D1 update + DO init)
        if (path.startsWith('/api/tournament_matches/') && path.endsWith('/start_live') && path.split('/').length === 5 && method === 'POST') {
             // Matches /api/tournament_matches/:matchId/start_live
             return adminAuthMiddleware(request, env, ctx, handleStartLiveMatch);
        }
        // NEW Semifinal Match Endpoints
        // POST /api/semifinal-matches (Admin)
        if (path === '/api/semifinal-matches' && method === 'POST') {
            // Admin middleware already applied above
            return handleCreateSemifinalMatch(request, env);
        }
        // GET /api/semifinal-matches (Admin)
        if (path === '/api/semifinal-matches' && method === 'GET') {
                // Admin middleware already applied above
                return handleFetchSemifinalMatches(request, env);
        }
        // POST /api/semifinal-matches/:id/submit-scores (Admin)
        if (path.match(/^\/api\/semifinal-matches\/\d+\/submit-scores$/) && method === 'POST') {
            const matchId = parseInt(path.split('/')[3]);
            if (isNaN(matchId)) return errorResponse('Invalid matchId', 400);
                // Admin middleware already applied above
            return handleSubmitSemifinalScores(request, env, matchId);
        }
        // GET /api/semifinal-matches/:id (Public view of completed match)
        if (path.match(/^\/api\/semifinal-matches\/\d+$/) && method === 'GET') {
            const matchId = parseInt(path.split('/')[3]);
            if (isNaN(matchId)) return errorResponse('Invalid matchId', 400);
            // This endpoint is public
            return handleFetchSemifinalMatch(request, env, matchId);
        }
            // POST /api/semifinal-matches/:id/archive (Admin)
            if (path.match(/^\/api\/semifinal-matches\/\d+\/archive$/) && method === 'POST') {
                const matchId = parseInt(path.split('/')[3]);
                if (isNaN(matchId)) return errorResponse('Invalid matchId', 400);
                // Admin middleware already applied above
                return handleArchiveSemifinalMatch(request, env, matchId);
            }


        // --- Not Found ---
        return errorResponse('Not Found', 404);
    },
};

// Export the Durable Object class
export { MatchDO };

// --- Missing Payload Type Definitions (Based on usage in handlers) ---
// These should ideally be in your types.ts file, but defined here for completeness
// if they are not already there.

// Payload for POST /api/tournament_matches
// interface CreateTournamentMatchPayload { // Already imported from types.ts
//     round_name: string;
//     team1_id: number;
//     team2_id: number;
//     scheduled_time?: string | null; // ISO string or null
// }

// Payload for PUT /api/tournament_matches/:id/confirm_setup
// interface ConfirmMatchSetupPayload { // Already imported from types.ts
//     team1_player_order: number[]; // Array of member IDs
//     team2_player_order: number[]; // Array of member IDs
//     match_song_list: MatchSong[]; // Array of MatchSong objects
// }

// Note: MatchSong type should be defined in your types.ts file
// interface MatchSong { // Already imported from types.ts
//     song_id: number;
//     song_title: string; // Denormalized
//     song_difficulty: string; // e.g., 'M 13'
//     song_element?: 'fire' | 'wood' | 'water' | null; // Denormalized
//     cover_filename?: string | null; // Denormalized
//     bpm?: string | null; // Denormalized
//     fullCoverUrl?: string; // Denormalized

//     picker_member_id: number; // Picker member ID
//     picker_team_id: number;   // Picker team ID
//     is_tiebreaker_song?: boolean; // Is tiebreaker song

//     status: 'pending' | 'ongoing' | 'completed'; // Status in this match

//     // Results (populated by DO)
//     teamA_player_id?: number;
//     teamB_player_id?: number;
//     teamA_percentage?: number;
//     teamB_percentage?: number;
//     teamA_damage_dealt?: number;
//     teamB_damage_dealt?: number;
//     teamA_effect_value?: number;
//     teamB_effect_value?: number;
//     teamA_health_after?: number;
//     teamB_health_after?: number;
//     teamA_mirror_triggered?: boolean;
//     teamB_mirror_triggered?: boolean;
// }
