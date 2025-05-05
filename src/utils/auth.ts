// src/utils/auth.ts

import { createRemoteJWKSet, jwtVerify } from 'jose';
import { Env, AuthenticatedRequest, Member, KindeUser } from '../types';
import { apiError } from '../index'; // Assuming apiError is exported from index.ts

// Define JWKS outside the fetch handler to reuse the connection and cache keys
let kindeJwks: ReturnType<typeof createRemoteJWKSet> | undefined;

// Helper to verify Kinde Access Token using jose
// Returns Kinde user ID and claims if valid, null otherwise
async function verifyKindeToken(env: Env, token: string): Promise<{ userId: string, claims: any } | null> {
    if (!env.KINDE_ISSUER_URL) {
        console.error("KINDE_ISSUER_URL not configured in Worker secrets.");
        return null;
    }

    // Initialize JWKS set if not already done
    if (!kindeJwks) {
        try {
             // Kinde's JWKS endpoint is at /.well-known/jwks relative to the issuer URL
             kindeJwks = createRemoteJWKSet(new URL(`${env.KINDE_ISSUER_URL}/.well-known/jwks`));
             console.log("Kinde JWKS set created.");
        } catch (e) {
             console.error("Failed to create Kinde JWKS set:", e);
             return null; // Cannot verify without JWKS
        }
    }

    try {
        // Use jwtVerify to verify the token signature and claims
        const { payload, protectedHeader } = await jwtVerify(token, kindeJwks, {
            issuer: env.KINDE_ISSUER_URL, // Ensure the token was issued by your Kinde domain
            // audience: 'your_api_audience', // Optional: If you configured an API audience in Kinde
            // You might also check 'typ' or other claims if needed
        });

        // Kinde's user ID is typically stored in the 'sub' claim of the token payload
        if (!payload.sub) {
             console.error("Kinde token payload missing 'sub' claim.");
             return null;
        }

        // Return the user ID and the full payload claims
        return { userId: payload.sub, claims: payload };

    } catch (e) {
        console.error("Error verifying Kinde token with jose:", e);
        // This catch block will handle various JWT errors like:
        // - JWSInvalid: Invalid signature
        // - JWTExpired: Token expired
        // - JWTClaimValidationFailed: Issuer or audience mismatch
        // You can add more specific error logging based on the error type if needed.
        return null; // Token is invalid or expired
    }
}

// Middleware to authenticate Kinde user and fetch member data
// Attaches kindeUser, member, and isAdmin to the request object
// Returns Response if authentication fails, otherwise returns void to continue
export async function authMiddleware(request: AuthenticatedRequest, env: Env): Promise<Response | void> {
    // 1. Get Token from Cookie or Header (matching App 1 frontend)
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
        console.warn('Authentication required: No token found.');
        return apiError('Authentication required.', 401);
    }

    // 2. Verify Token using Kinde's JWKS
    const verificationResult = await verifyKindeToken(env, token);
    if (!verificationResult) {
        console.warn("Kinde token verification failed.");
        // Clear cookies on verification failure (best effort for HttpOnly)
        const response = apiError('Authentication failed: Invalid or expired token.', 401);
        response.headers.append('Set-Cookie', 'kinde_access_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax');
        response.headers.append('Set-Cookie', 'kinde_refresh_token=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax');
        return response;
    }

    const kindeUserId = verificationResult.userId;
    const kindeClaims = verificationResult.claims;

    // Populate request with basic Kinde user info
    request.kindeUser = {
      id: kindeUserId,
      email: kindeClaims.email as string,
      given_name: kindeClaims.given_name as string | undefined,
      family_name: kindeClaims.family_name as string | undefined,
      // Map other claims as needed
    };

    // 3. Fetch Member data from D1 using kinde_user_id
    try {
        const memberResult = await env.DB.prepare('SELECT * FROM members WHERE kinde_user_id = ?').bind(kindeUserId).first<Member>();

        if (!memberResult) {
            // User is authenticated via Kinde but not registered in your DB
            console.warn(`Kinde user ${kindeUserId} not found in members table.`);
            // Depending on your flow, this might be 403 or redirect to registration
            // For API, 403 is safer. Frontend should handle redirect if needed.
            return apiError('Forbidden: User not registered in the system.', 403);
        }

        // Populate request with Member data and admin status
        request.member = memberResult;
        request.isAdmin = memberResult.is_admin === 1;
        console.log(`User ${kindeUserId} authenticated. Member ID: ${memberResult.id}, IsAdmin: ${request.isAdmin}`);

        // Continue to the next handler
        return;

    } catch (e) {
        console.error(`Database error fetching member for Kinde ID ${kindeUserId}:`, e);
        return apiError('Internal server error during authentication.', 500, e);
    }
}

// Middleware to check if the authenticated user is an admin
// Requires authMiddleware to have run first
// Returns Response if not admin, otherwise returns void to continue
export async function adminAuthMiddleware(request: AuthenticatedRequest, env: Env): Promise<Response | void> {
    // authMiddleware must run before this to populate request.isAdmin
    // We assume authMiddleware has already been applied in the router chain
    if (!request.isAdmin) {
        console.warn(`Admin access denied for user ${request.kindeUser?.id || 'unknown'}.`);
        return apiError('Forbidden: Administrator access required.', 403);
    }
    console.log(`Admin access granted for user ${request.kindeUser?.id}.`);
    // Continue to the next handler
    return;
}

// Helper function to get Kinde User ID (used by authMiddleware internally)
// Exported if needed elsewhere, but authMiddleware is the primary way to get user info
// async function getAuthenticatedKindeUser(request: Request, env: Env): Promise<string | null> { ... } // Already implemented inside authMiddleware logic
