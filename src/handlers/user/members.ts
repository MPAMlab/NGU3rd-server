// src/handlers/user/members.ts

import { AuthenticatedRequest, Env, Member } from '../../types';
import { apiResponse, apiError } from '../../index'; // Assuming these are exported from index.ts

// GET /api/members/me (Requires Kinde Auth)
export async function handleFetchMe(request: AuthenticatedRequest, env: Env): Promise<Response> {
    console.log(`Handling /api/members/me request for Kinde ID: ${request.kindeUser?.id}`);
    // Authentication and member lookup is already done by authMiddleware
    // request.member is guaranteed to be non-null if authMiddleware succeeded and found a member

    if (!request.member) {
        // This case should ideally not happen if authMiddleware passed,
        // but as a fallback, return not found or not registered.
        console.error("Auth middleware passed but request.member is null in handleFetchMe.");
        return apiResponse({ member: null, message: "User not registered or internal error." }, 500); // Or 404/403 depending on desired behavior
    }

    // Return the member data found by the middleware
    return apiResponse({ member: request.member }, 200);
}

// TODO: Add other user-specific handlers if needed (e.g., update profile, view achievements)
// PATCH /api/members/me (Allow user to update their own profile fields like nickname, avatar)
// This would be similar to handleUserPatchMember from App 1 Worker, but targeting the authenticated user's member record.
