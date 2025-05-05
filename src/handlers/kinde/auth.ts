// src/handlers/kinde/auth.ts

import { Env } from '../../types';
import { apiResponse, apiError, CORS_HEADERS } from '../../index'; // Assuming these are exported from index.ts
import { createRemoteJWKSet, jwtVerify } from 'jose'; // Needed for ID token decoding if you want user info back

// Define JWKS outside the handler to reuse the connection and cache keys
let kindeJwks: ReturnType<typeof createRemoteJWKSet> | undefined;

async function getKindeJwks(env: Env): Promise<ReturnType<typeof createRemoteJWKSet>> {
    if (!env.KINDE_ISSUER_URL) {
        throw new Error("KINDE_ISSUER_URL not configured.");
    }
    if (!kindeJwks) {
        kindeJwks = createRemoteJWKSet(new URL(`${env.KINDE_ISSUER_URL}/.well-known/jwks`));
    }
    return kindeJwks;
}


// GET /login (Initiate Kinde login redirect)
export function handleKindeLogin(request: Request, env: Env): Response {
    if (!env.KINDE_ISSUER_URL || !env.KINDE_CLIENT_ID || !env.KINDE_REDIRECT_URI) {
        console.error("Kinde login configuration missing.");
        return apiError('Server configuration error for login.', 500);
    }

    const authUrl = new URL(`${env.KINDE_ISSUER_URL}/oauth2/auth`);
    authUrl.searchParams.set('client_id', env.KINDE_CLIENT_ID);
    authUrl.searchParams.set('response_type', 'code'); // Using Authorization Code Flow with PKCE
    authUrl.searchParams.set('redirect_uri', env.KINDE_REDIRECT_URI);
    authUrl.searchParams.set('scope', 'openid profile email offline'); // Request necessary scopes
    // State and Code Challenge are handled by the frontend PKCE flow before redirecting here
    // The frontend will generate state and verifier, store them, and include state and code_challenge in the redirect URL it constructs.
    // This backend endpoint just needs to redirect to Kinde.

    // Optional: Add prompt=create for signup flow if needed, based on frontend logic
    const url = new URL(request.url);
    if (url.searchParams.get('prompt') === 'create') {
         authUrl.searchParams.set('prompt', 'create');
    }


    console.log("Redirecting to Kinde auth URL:", authUrl.toString());
    return Response.redirect(authUrl.toString(), 302);
}

// POST /api/kinde/callback (Handle callback from frontend)
// This receives the code and verifier from the frontend via POST body
export async function handleKindeCallback(request: Request, env: Env): Promise<Response> {
    console.log('Handling POST /api/kinde/callback request...');
    const body = await request.json().catch(() => null);
    if (!body) return apiError('Invalid or missing JSON body.', 400);

    const { code, code_verifier, redirect_uri } = body; // Expecting these from frontend POST

    if (!code || !code_verifier || !redirect_uri) {
        return apiError('Missing code, code_verifier, or redirect_uri in callback request body.', 400);
    }

    if (!env.KINDE_CLIENT_ID || !env.KINDE_CLIENT_SECRET || !env.KINDE_ISSUER_URL || env.KINDE_REDIRECT_URI !== redirect_uri) {
         console.error("Kinde secrets or redirect_uri mismatch in Worker config.");
         // Avoid leaking exact reason, just indicate config error
         return apiError('Server configuration error or redirect URI mismatch.', 500);
    }

    try {
        // Exchange code for tokens with Kinde
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
                code_verifier: code_verifier, // PKCE verifier
                grant_type: 'authorization_code',
                redirect_uri: redirect_uri, // Must match the one used in the initial redirect
            }),
        });

        const tokenData = await tokenResponse.json();

        if (!tokenResponse.ok) {
            console.error('Kinde token exchange failed:', tokenResponse.status, tokenData);
            return apiError(tokenData.error_description || tokenData.error || 'Failed to exchange authorization code for tokens.', tokenResponse.status);
        }

        // Successfully got tokens!
        const { access_token, id_token, refresh_token, expires_in } = tokenData;

        const headers = new Headers(CORS_HEADERS);
        const url = new URL(request.url); // Use request URL to determine secure/domain for cookie
        const secure = url.protocol === 'https:' ? '; Secure' : '';
        const domain = url.hostname; // Ensure this matches the domain where cookies should be set

        // Set Access Token cookie (HttpOnly)
        headers.append('Set-Cookie', `kinde_access_token=${access_token}; HttpOnly; Path=/; Max-Age=${expires_in}; SameSite=Lax${secure}; Domain=${domain}`);

        // Set Refresh Token cookie (HttpOnly)
        if (refresh_token) {
             const refreshTokenMaxAge = 30 * 24 * 60 * 60; // 30 days
             headers.append('Set-Cookie', `kinde_refresh_token=${refresh_token}; HttpOnly; Path=/; Max-Age=${refreshTokenMaxAge}; SameSite=Lax${secure}; Domain=${domain}`);
        }

        // Decode ID token for basic user info to return to frontend
        let userInfo: KindeUser | {} = {};
        if (id_token) {
            try {
                // Verify ID token signature (optional but recommended)
                const jwks = await getKindeJwks(env);
                const { payload } = await jwtVerify(id_token, jwks, {
                    issuer: env.KINDE_ISSUER_URL,
                    // audience: env.KINDE_CLIENT_ID, // ID token audience is usually client_id
                });

                userInfo = {
                    id: payload.sub as string, // Kinde User ID
                    email: payload.email as string,
                    given_name: payload.given_name as string | undefined,
                    family_name: payload.family_name as string | undefined,
                    // Add other claims you requested in scope (profile, etc.)
                };
                 console.log("ID token decoded and verified. User info:", userInfo);

            } catch (e) {
                console.error("Failed to decode or verify ID token payload:", e);
                // Continue without user info if ID token is problematic
            }
        } else {
             console.warn("No ID token received in callback.");
        }


        return new Response(JSON.stringify({ success: true, user: userInfo }), {
            status: 200,
            headers: headers, // Include Set-Cookie headers
        });

    } catch (kindeError) {
        console.error('Error during Kinde token exchange:', kindeError);
        return apiError('Failed to communicate with authentication server.', 500, kindeError);
    }
}

// GET /logout (Initiate Kinde logout redirect and clear cookies)
export function handleKindeLogout(request: Request, env: Env): Response {
    if (!env.KINDE_ISSUER_URL || !env.LOGOUT_REDIRECT_TARGET_URL) {
        console.error("Kinde logout configuration missing.");
        return apiError('Server configuration error for logout.', 500);
    }

    const logoutUrl = new URL(`${env.KINDE_ISSUER_URL}/logout`);
    logoutUrl.searchParams.set('redirect', env.LOGOUT_REDIRECT_TARGET_URL);

    const response = Response.redirect(logoutUrl.toString(), 302);

    // Clear the authentication cookies for the main site domain
    // These headers are added to the redirect response
    const url = new URL(request.url);
    const secure = url.protocol === 'https:' ? '; Secure' : '';
    const domain = url.hostname;

    response.headers.append('Set-Cookie', `kinde_access_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; SameSite=Lax${secure}; Domain=${domain}`);
    response.headers.append('Set-Cookie', `kinde_refresh_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; SameSite=Lax${secure}; Domain=${domain}`);

    console.log("Initiating Kinde logout and clearing cookies.");

    return response;
}
