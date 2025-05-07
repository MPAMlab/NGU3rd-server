// src/index.ts

import { MatchDO } from './durable-objects/matchDo';
import type {
    Env,
    RoundArchive,
    MatchArchiveSummary,
    Team,
    Member,
    TournamentMatch,
    CreateTournamentMatchPayload,
    BulkTeamRow,
    BulkMemberRow,
    BulkTournamentMatchRow,
    MatchState, // Import MatchState if needed in worker (e.g., for type assertions)
    MatchScheduleData // Import the new type
} from './types'; // Import all necessary types

// Helper function to get the singleton MatchDO ID (Used for unscheduled matches)
// Note: Scheduled matches will use a DO ID derived from their tournamentMatchId
function getSingletonMatchId(env: Env): DurableObjectId {
  // Using a fixed name for the *current* live match DO instance (for unscheduled flow)
  return env.MATCH_DO.idFromName("singleton-match-instance");
}

// Helper to determine winner based on scores (duplicated from DO for D1 updates)
function determineWinner(state: { team_a_score: number; team_b_score: number; team_a_name: string; team_b_name: string }): string | null {
    if (state.team_a_score > state.team_b_score) {
        return state.team_a_name || '队伍A';
    } else if (state.team_b_score > state.team_a_score) { // FIX: Corrected comparison here
        return state.team_b_name || '队伍B';
    } else {
        return null; // Draw or undecided
    }
}

// Helper to parse player order string (e.g., '1,2,3') into an array of 1-based numbers
function parsePlayerOrder(orderString: string | null | undefined): number[] {
    if (!orderString) return [];
    // Filter for valid numbers 1-3 (assuming 3 players per team)
    return orderString.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n) && n >= 1 && n <= 3);
}


export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    // Note: matchStub here refers to the *singleton* DO, used for unscheduled matches.
    // Scheduled matches will get their stub based on tournamentMatchId.
    const singletonMatchId = getSingletonMatchId(env);
    const singletonMatchStub = env.MATCH_DO.get(singletonMatchId);


    // --- Route requests to the Durable Object (Singleton Instance) ---
    // These endpoints are for the *current* unscheduled live match managed by the singleton DO

    // WebSocket requests are forwarded to the DO
    if (url.pathname === '/api/match/websocket') {
      return singletonMatchStub.fetch(new Request(url.origin + "/websocket", request));
    }

    // Forward specific API calls related to the *current* match state to the DO's internal endpoints
    if (url.pathname === '/api/match/state' && request.method === 'GET') {
        return singletonMatchStub.fetch(new Request(url.origin + "/state", request));
    }
    if (url.pathname === '/api/match/update' && request.method === 'POST') {
        return singletonMatchStub.fetch(new Request(url.origin + "/update", request));
    }
    if (url.pathname === '/api/match/archive-round' && request.method === 'POST') {
        return singletonMatchStub.fetch(new Request(url.origin + "/internal/archive-round", request));
    }
    if (url.pathname === '/api/match/next-round' && request.method === 'POST') {
        return singletonMatchStub.fetch(new Request(url.origin + "/internal/next-round", request));
    }
    if (url.pathname === '/api/match/archive-match' && request.method === 'POST') {
        return singletonMatchStub.fetch(new Request(url.origin + "/internal/archive-match", request));
    }
     // This is the old 'new match' endpoint, now for unscheduled matches
     if (url.pathname === '/api/match/new-match' && request.method === 'POST') {
        return singletonMatchStub.fetch(new Request(url.origin + "/internal/new-match", request));
    }


    // --- Handle D1 Queries/Updates directly in the Worker ---

    // --- Teams Management Endpoints ---
    if (url.pathname === '/api/teams') {
        if (request.method === 'POST') {
            // Create Team
            try {
                const teamData = await request.json<Omit<Team, 'id' | 'created_at' | 'current_health' | 'has_revive_mirror' | 'status'>>();
                 if (!teamData.code || teamData.code.length !== 4 || !teamData.name) {
                     return new Response(JSON.stringify({ error: "Team code (4 chars) and name are required." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 // Check if code already exists
                 const existingTeam = await env.DB.prepare("SELECT id FROM teams WHERE code = ?").bind(teamData.code).first();
                 if (existingTeam) {
                      return new Response(JSON.stringify({ error: `Team code '${teamData.code}' already exists.` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }


                const stmt = env.DB.prepare("INSERT INTO teams (code, name, created_at, current_health, has_revive_mirror, status) VALUES (?, ?, ?, ?, ?, ?)");
                const result = await stmt.bind(
                    teamData.code,
                    teamData.name,
                    Math.floor(Date.now() / 1000), // Use integer timestamp
                    100, // Default current_health
                    1, // Default has_revive_mirror
                    'active' // Default status
                ).run();

                if (result.success) {
                    // Fetch the newly created team to return it with its ID
                    const newTeam = await env.DB.prepare("SELECT * FROM teams WHERE id = ?").bind(result.meta.last_row_id).first<Team>();
                    return new Response(JSON.stringify({ success: true, message: "Team created.", team: newTeam }), { status: 201, headers: { 'Content-Type': 'application/json' } });
                } else {
                    console.error("D1 create team error:", result.error);
                    return new Response(JSON.stringify({ error: "Failed to create team", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                }
            } catch (e: any) {
                console.error("Worker exception during create team:", e.stack); // Use e.stack for better debugging
                return new Response(JSON.stringify({ error: `Exception creating team: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        } else if (request.method === 'GET') {
            // Get All Teams
            try {
                const { results } = await env.DB.prepare("SELECT * FROM teams ORDER BY code ASC").all<Team>();
                return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
            } catch (e: any) {
                console.error("D1 get teams error:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: "Failed to retrieve teams", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }
    } else if (url.pathname.startsWith('/api/teams/')) {
         const pathParts = url.pathname.split('/');
         const teamId = parseInt(pathParts[3], 10); // e.g., /api/teams/{id}

         if (isNaN(teamId)) {
             return new Response(JSON.stringify({ error: "Invalid team ID in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
         }

         if (request.method === 'GET' && pathParts.length === 4) { // Ensure it's just /api/teams/{id}
             // Get Single Team
             try {
                 const { results } = await env.DB.prepare("SELECT * FROM teams WHERE id = ?").bind(teamId).all<Team>();
                 if (results && results.length > 0) {
                     return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     return new Response(JSON.stringify({ error: "Team not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("D1 get team error:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: "Failed to retrieve team", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         } else if (request.method === 'PUT' && pathParts.length === 4) { // Ensure it's just /api/teams/{id}
             // Update Team
             try {
                 // Allow updating name, health, mirror, status. Prevent changing code.
                 const updates = await request.json<Partial<Omit<Team, 'id' | 'code' | 'created_at'>>>();
                 delete updates.code; // Prevent changing code via PUT

                 const stmt = env.DB.prepare(
                     `UPDATE teams SET
                        name = COALESCE(?, name),
                        current_health = COALESCE(?, current_health),
                        has_revive_mirror = COALESCE(?, has_revive_mirror),
                        status = COALESCE(?, status)
                      WHERE id = ?`
                 );
                 const result = await stmt.bind(
                     updates.name,
                     updates.current_health,
                     updates.has_revive_mirror,
                     updates.status,
                     teamId
                 ).run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Team not found or no changes made." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     const updatedTeam = await env.DB.prepare("SELECT * FROM teams WHERE id = ?").bind(teamId).first<Team>();
                     return new Response(JSON.stringify({ success: true, message: "Team updated.", team: updatedTeam }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 update team error:", result.error);
                      // Check for unique constraint violation (shouldn't happen if code is deleted, but good practice)
                      if (result.error?.includes("UNIQUE constraint failed: teams.code")) {
                         return new Response(JSON.stringify({ error: `Team code already exists (should not happen via PUT).` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                      }
                     return new Response(JSON.stringify({ error: "Failed to update team", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during update team:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception updating team: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         } else if (request.method === 'DELETE' && pathParts.length === 4) { // Ensure it's just /api/teams/{id}
             // Delete Team
             try {
                 // Check if team is used in tournament_matches or has members
                 const checkMatchesStmt = env.DB.prepare("SELECT COUNT(*) as count FROM tournament_matches WHERE team1_id = ? OR team2_id = ?").bind(teamId, teamId);
                 const { results: checkMatchesResults } = await checkMatchesStmt.all<{ count: number }>();
                 if (checkMatchesResults && checkMatchesResults[0].count > 0) {
                      return new Response(JSON.stringify({ error: "Cannot delete team: It is used in scheduled matches." }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }

                 // Need team_code to check members table
                 const teamEntry = await env.DB.prepare("SELECT code FROM teams WHERE id = ?").bind(teamId).first<{ code: string }>();
                 if (!teamEntry) {
                      return new Response(JSON.stringify({ success: false, message: "Team not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }
                 const checkMembersStmt = env.DB.prepare("SELECT COUNT(*) as count FROM members WHERE team_code = ?").bind(teamEntry.code);
                 const { results: checkMembersResults } = await checkMembersStmt.all<{ count: number }>();
                 if (checkMembersResults && checkMembersResults[0].count > 0) {
                      return new Response(JSON.stringify({ error: "Cannot delete team: It has associated members. Delete members first." }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }


                 const stmt = env.DB.prepare("DELETE FROM teams WHERE id = ?").bind(teamId);
                 const result = await stmt.run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Team not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     return new Response(JSON.stringify({ success: true, message: "Team deleted." }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 delete team error:", result.error);
                     return new Response(JSON.stringify({ error: "Failed to delete team", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during delete team:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception deleting team: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         }
    }

    // --- Bulk Import Teams Endpoint ---
    if (url.pathname === '/api/teams/bulk' && request.method === 'POST') {
        try {
            const teamsData = await request.json<BulkTeamRow[]>();
            if (!Array.isArray(teamsData)) {
                 return new Response(JSON.stringify({ error: "Invalid payload: Expected an array of teams." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }

            // Basic validation for each row and check for duplicate codes in the input
            const codes = new Set<string>();
            for (const team of teamsData) {
                 if (!team.code || team.code.length !== 4 || !team.name) {
                     return new Response(JSON.stringify({ error: `Invalid team data found in input: code='${team.code}', name='${team.name}'. Code must be 4 chars and name is required.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 if (codes.has(team.code)) {
                      return new Response(JSON.stringify({ error: `Duplicate team code '${team.code}' found in the input data.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 codes.add(team.code);
            }

            // Check for existing team codes in the database
            if (codes.size > 0) {
                 const codeArray = Array.from(codes);
                 const existingTeamsStmt = env.DB.prepare(`SELECT code FROM teams WHERE code IN (${codeArray.map(() => '?').join(',')})`).bind(...codeArray);
                 const { results: existingTeams } = await existingTeamsStmt.all<{ code: string }>();
                 const existingTeamCodes = new Set(existingTeams?.map(t => t.code) || []);

                 if (existingTeamCodes.size > 0) {
                      return new Response(JSON.stringify({ error: `Bulk import failed: Team codes already exist: ${Array.from(existingTeamCodes).join(', ')}. Please remove existing teams or update them individually.` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }
            }


            const stmt = env.DB.prepare("INSERT INTO teams (code, name, created_at, current_health, has_revive_mirror, status) VALUES (?, ?, ?, ?, ?, ?)");
            const now = Math.floor(Date.now() / 1000);
            const insertBatch = teamsData.map(team => {
                 return stmt.bind(team.code, team.name, now, 100, 1, 'active');
            });

            // Use batch for efficiency
            const results = await env.DB.batch(insertBatch);

            // Check results for errors (batch might succeed partially, though we pre-checked)
            const errors = results.filter(r => !r.success);
            const successCount = results.length - errors.length;

            if (errors.length > 0) {
                 console.error("D1 bulk create teams errors:", errors);
                 return new Response(JSON.stringify({
                     success: successCount > 0,
                     message: `Bulk import completed with errors. Successfully imported ${successCount} teams. Failed to import ${errors.length} teams.`,
                     errors: errors.map(e => e.error)
                 }), { status: successCount > 0 ? 207 : 500, headers: { 'Content-Type': 'application/json' } }); // 207 Multi-Status
            }

            return new Response(JSON.stringify({ success: true, message: `Bulk import successful. ${successCount} teams imported.` }), { status: 201, headers: { 'Content-Type': 'application/json' } });

        } catch (e: any) {
            console.error("Worker exception during bulk create teams:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: `Exception during bulk import teams: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // --- Members Management Endpoints ---
    // Add endpoints for /api/members and /api/members/:id (GET, POST, PUT, DELETE)
    // Add endpoint for /api/teams/:team_code/members (GET)

    if (url.pathname === '/api/members') {
        if (request.method === 'POST') {
            // Create Member
            try {
                const memberData = await request.json<Omit<Member, 'id' | 'joined_at' | 'updated_at'>>();
                 if (!memberData.team_code || !memberData.nickname) {
                     return new Response(JSON.stringify({ error: "Team code and nickname are required for a member." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 // Optional: Validate team_code exists
                 const teamExists = await env.DB.prepare("SELECT id FROM teams WHERE code = ?").bind(memberData.team_code).first();
                 if (!teamExists) {
                      return new Response(JSON.stringify({ error: `Team code '${memberData.team_code}' not found.` }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }


                const stmt = env.DB.prepare("INSERT INTO members (team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                const now = Math.floor(Date.now() / 1000);
                const result = await stmt.bind(
                    memberData.team_code,
                    memberData.color || null,
                    memberData.job || null,
                    memberData.maimai_id || null,
                    memberData.nickname,
                    memberData.qq_number || null,
                    memberData.avatar_url || null,
                    now, // joined_at
                    now, // updated_at (initial)
                    memberData.kinde_user_id || null,
                    memberData.is_admin ?? 0 // Default is_admin to 0
                ).run();

                if (result.success) {
                    const newMember = await env.DB.prepare("SELECT * FROM members WHERE id = ?").bind(result.meta.last_row_id).first<Member>();
                    return new Response(JSON.stringify({ success: true, message: "Member created.", member: newMember }), { status: 201, headers: { 'Content-Type': 'application/json' } });
                } else {
                    console.error("D1 create member error:", result.error);
                    return new Response(JSON.stringify({ error: "Failed to create member", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                }
            } catch (e: any) {
                console.error("Worker exception during create member:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: `Exception creating member: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        } else if (request.method === 'GET') {
            // Get All Members (consider pagination if many members)
            try {
                const { results } = await env.DB.prepare("SELECT * FROM members ORDER BY team_code ASC, id ASC").all<Member>();
                return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
            } catch (e: any) {
                console.error("D1 get members error:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: "Failed to retrieve members", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }
    } else if (url.pathname.startsWith('/api/members/')) {
         const pathParts = url.pathname.split('/');
         const memberId = parseInt(pathParts[3], 10); // e.g., /api/members/{id}

         if (isNaN(memberId)) {
             return new Response(JSON.stringify({ error: "Invalid member ID in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
         }

         if (request.method === 'GET' && pathParts.length === 4) { // Ensure it's just /api/members/{id}
             // Get Single Member
             try {
                 const { results } = await env.DB.prepare("SELECT * FROM members WHERE id = ?").bind(memberId).all<Member>();
                 if (results && results.length > 0) {
                     return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     return new Response(JSON.stringify({ error: "Member not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("D1 get member error:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: "Failed to retrieve member", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         } else if (request.method === 'PUT' && pathParts.length === 4) { // Ensure it's just /api/members/{id}
             // Update Member
             try {
                 // Allow updating most fields except id and joined_at
                 const updates = await request.json<Partial<Omit<Member, 'id' | 'joined_at' | 'updated_at'>>>();
                 // Prevent changing team_code via PUT if you want it immutable after creation
                 // delete updates.team_code;

                 const stmt = env.DB.prepare(
                     `UPDATE members SET
                        team_code = COALESCE(?, team_code), -- Keep team_code if not provided
                        color = COALESCE(?, color),
                        job = COALESCE(?, job),
                        maimai_id = COALESCE(?, maimai_id),
                        nickname = COALESCE(?, nickname),
                        qq_number = COALESCE(?, qq_number),
                        avatar_url = COALESCE(?, avatar_url),
                        updated_at = ?, -- Always update updated_at
                        kinde_user_id = COALESCE(?, kinde_user_id),
                        is_admin = COALESCE(?, is_admin)
                      WHERE id = ?`
                 );
                 const now = Math.floor(Date.now() / 1000);
                 const result = await stmt.bind(
                     updates.team_code, // This will be null if not provided, COALESCE keeps old value
                     updates.color,
                     updates.job,
                     updates.maimai_id,
                     updates.nickname,
                     updates.qq_number,
                     updates.avatar_url,
                     now, // updated_at
                     updates.kinde_user_id,
                     updates.is_admin,
                     memberId
                 ).run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Member not found or no changes made." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     const updatedMember = await env.DB.prepare("SELECT * FROM members WHERE id = ?").bind(memberId).first<Member>();
                     return new Response(JSON.stringify({ success: true, message: "Member updated.", member: updatedMember }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 update member error:", result.error);
                     return new Response(JSON.stringify({ error: "Failed to update member", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during update member:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception updating member: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         } else if (request.method === 'DELETE' && pathParts.length === 4) { // Ensure it's just /api/members/{id}
             // Delete Member
             try {
                 const stmt = env.DB.prepare("DELETE FROM members WHERE id = ?").bind(memberId);
                 const result = await stmt.run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Member not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     return new Response(JSON.stringify({ success: true, message: "Member deleted." }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 delete member error:", result.error);
                     return new Response(JSON.stringify({ error: "Failed to delete member", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during delete member:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception deleting member: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         }
    } else if (url.pathname.startsWith('/api/teams/') && url.pathname.endsWith('/members') && request.method === 'GET') {
        // Get Members by Team Code
        const pathParts = url.pathname.split('/');
        const teamCode = pathParts[3]; // e.g., /api/teams/{team_code}/members

        if (!teamCode) {
             return new Response(JSON.stringify({ error: "Team code missing in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }

        try {
            // Optional: Validate team_code exists
            const teamExists = await env.DB.prepare("SELECT id FROM teams WHERE code = ?").bind(teamCode).first();
            if (!teamExists) {
                 return new Response(JSON.stringify({ error: `Team code '${teamCode}' not found.` }), { status: 404, headers: { 'Content-Type': 'application/json' } });
            }

            const stmt = env.DB.prepare("SELECT * FROM members WHERE team_code = ? ORDER BY id ASC").bind(teamCode); // Order by ID for consistent player order
            const { results } = await stmt.all<Member>();
            return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
        } catch (e: any) {
            console.error("D1 get members by team error:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: "Failed to retrieve members for team", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }

    // --- Bulk Import Members Endpoint ---
     if (url.pathname === '/api/members/bulk' && request.method === 'POST') {
        try {
            const membersData = await request.json<BulkMemberRow[]>();
            if (!Array.isArray(membersData)) {
                 return new Response(JSON.stringify({ error: "Invalid payload: Expected an array of members." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }

            // Validate team codes exist before batch insert
            const teamCodes = [...new Set(membersData.map(m => m.team_code))]; // Get unique team codes
            if (teamCodes.length === 0) {
                 return new Response(JSON.stringify({ error: "No member data provided or missing team_code." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }

            const teamCheckStmt = env.DB.prepare(`SELECT code FROM teams WHERE code IN (${teamCodes.map(() => '?').join(',')})`).bind(...teamCodes);
            const { results: existingTeams } = await teamCheckStmt.all<{ code: string }>();
            const existingTeamCodes = new Set(existingTeams?.map(t => t.code) || []);

            const invalidTeamCodes = teamCodes.filter(code => !existingTeamCodes.has(code));
            if (invalidTeamCodes.length > 0) {
                 return new Response(JSON.stringify({ error: `Bulk import failed: Invalid team codes found: ${invalidTeamCodes.join(', ')}. Please ensure teams exist before importing members.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }

            // Basic validation for each member row
             for (const member of membersData) {
                 if (!member.team_code || !member.nickname) {
                      return new Response(JSON.stringify({ error: `Invalid member data found in input: team_code='${member.team_code}', nickname='${member.nickname}'. Team code and nickname are required.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
             }


            const stmt = env.DB.prepare("INSERT INTO members (team_code, color, job, maimai_id, nickname, qq_number, avatar_url, joined_at, updated_at, kinde_user_id, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            const now = Math.floor(Date.now() / 1000);
            const insertBatch = membersData.map(member => {
                 // Parse is_admin string to number (0 or 1)
                 const isAdmin = member.is_admin !== undefined ? (parseInt(member.is_admin, 10) > 0 ? 1 : 0) : 0;

                 return stmt.bind(
                     member.team_code,
                     member.color || null,
                     member.job || null,
                     member.maimai_id || null,
                     member.nickname,
                     member.qq_number || null,
                     member.avatar_url || null,
                     now, // joined_at
                     now, // updated_at (initial)
                     member.kinde_user_id || null,
                     isAdmin
                 );
            });

            // Use batch for efficiency
            const results = await env.DB.batch(insertBatch);

            // Check results for errors (batch might succeed partially)
            const errors = results.filter(r => !r.success);
            const successCount = results.length - errors.length;

            if (errors.length > 0) {
                 console.error("D1 bulk create members errors:", errors);
                 return new Response(JSON.stringify({
                     success: successCount > 0, // Consider success if at least one row inserted
                     message: `Bulk import completed with errors. Successfully imported ${successCount} members. Failed to import ${errors.length} members.`,
                     errors: errors.map(e => e.error)
                 }), { status: successCount > 0 ? 207 : 500, headers: { 'Content-Type': 'application/json' } }); // 207 Multi-Status
            }

            return new Response(JSON.stringify({ success: true, message: `Bulk import successful. ${successCount} members imported.` }), { status: 201, headers: { 'Content-Type': 'application/json' } });

        } catch (e: any) {
            console.error("Worker exception during bulk create members:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: `Exception during bulk import members: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // --- Tournament Matches Management Endpoints ---
    if (url.pathname === '/api/tournament_matches') {
        if (request.method === 'POST') {
            // Create Tournament Match
            try {
                const matchData = await request.json<CreateTournamentMatchPayload>(); // Use the specific payload type
                 if (!matchData.tournament_round || matchData.match_number_in_round === undefined || matchData.team1_id === null || matchData.team2_id === null) {
                     return new Response(JSON.stringify({ error: "Tournament round, match number, team1_id, and team2_id are required." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 if (matchData.team1_id === matchData.team2_id) {
                      return new Response(JSON.stringify({ error: "队伍A和队伍B不能是同一支队伍。" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }

                 // Validate team IDs exist
                 const team1Exists = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(matchData.team1_id).first();
                 const team2Exists = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(matchData.team2_id).first();
                 if (!team1Exists || !team2Exists) {
                      return new Response(JSON.stringify({ error: "One or both team IDs not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }

                 // Check for unique constraint violation (round + number)
                 const existingMatch = await env.DB.prepare("SELECT id FROM tournament_matches WHERE tournament_round = ? AND match_number_in_round = ?").bind(matchData.tournament_round, matchData.match_number_in_round).first();
                 if (existingMatch) {
                      return new Response(JSON.stringify({ error: `Match ${matchData.match_number_in_round} in round '${matchData.tournament_round}' already exists.` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }


                const stmt = env.DB.prepare("INSERT INTO tournament_matches (tournament_round, match_number_in_round, team1_id, team2_id, team1_player_order, team2_player_order, scheduled_time, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                const result = await stmt.bind(
                    matchData.tournament_round,
                    matchData.match_number_in_round,
                    matchData.team1_id,
                    matchData.team2_id,
                    matchData.team1_player_order || null,
                    matchData.team2_player_order || null,
                    matchData.scheduled_time || null,
                    new Date().toISOString(),
                    'scheduled' // Default status
                ).run();

                if (result.success) {
                    // Fetch the newly created match to return it with its ID and team details
                     const newMatchStmt = env.DB.prepare(`
                         SELECT
                             tm.*,
                             t1.code AS team1_code, t1.name AS team1_name,
                             t2.code AS team2_code, t2.name AS team2_name
                         FROM tournament_matches tm
                         JOIN teams t1 ON tm.team1_id = t1.id
                         JOIN teams t2 ON tm.team2_id = t2.id
                         WHERE tm.id = ?
                     `);
                     // Use .first() as we expect only one result by ID
                     const newMatchResult = await newMatchStmt.bind(result.meta.last_row_id).first<TournamentMatch>();

                    return new Response(JSON.stringify({ success: true, message: "Tournament match created.", tournamentMatch: newMatchResult }), { status: 201, headers: { 'Content-Type': 'application/json' } });
                } else {
                    console.error("D1 create tournament match error:", result.error);
                    return new Response(JSON.stringify({ error: "Failed to create tournament match", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                }
            } catch (e: any) {
                console.error("Worker exception during create tournament match:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: `Exception creating tournament match: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        } else if (request.method === 'GET') {
            // Get All Tournament Matches (with team names and codes)
            try {
                // Join with teams table to get team names and codes
                const stmt = env.DB.prepare(`
                    SELECT
                        tm.*,
                        t1.code AS team1_code, t1.name AS team1_name,
                        t2.code AS team2_code, t2.name AS team2_name,
                        tw.code AS winner_team_code, tw.name AS winner_team_name
                    FROM tournament_matches tm
                    JOIN teams t1 ON tm.team1_id = t1.id
                    JOIN teams t2 ON tm.team2_id = t2.id
                    LEFT JOIN teams tw ON tm.winner_team_id = tw.id
                    ORDER BY tm.tournament_round, tm.match_number_in_round ASC
                `);
                // The TournamentMatch type includes the joined fields as optional, which is fine for .all()
                const { results } = await stmt.all<TournamentMatch>();

                return new Response(JSON.stringify(results || []), { headers: { 'Content-Type': 'application/json' } });
            } catch (e: any) {
                console.error("D1 get tournament matches error:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: "Failed to retrieve tournament matches", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        }
    } else if (url.pathname.startsWith('/api/tournament_matches/')) {
         const pathParts = url.pathname.split('/');
         // Check if the path is /api/tournament_matches/{id} or /api/tournament_matches/{id}/...
         const isSingleMatchEndpoint = pathParts.length >= 4 && !isNaN(parseInt(pathParts[3], 10));
         const tournamentMatchId = isSingleMatchEndpoint ? parseInt(pathParts[3], 10) : NaN;

         if (isNaN(tournamentMatchId) && isSingleMatchEndpoint) { // Only return error if ID was expected but invalid
             return new Response(JSON.stringify({ error: "Invalid tournament match ID in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
         }

         // --- Get Single Tournament Match ---
         if (request.method === 'GET' && pathParts.length === 4 && isSingleMatchEndpoint) {
             try {
                 const stmt = env.DB.prepare(`
                     SELECT
                         tm.*,
                         t1.code AS team1_code, t1.name AS team1_name,
                         t2.code AS team2_code, t2.name AS team2_name,
                         tw.code AS winner_team_code, tw.name AS winner_team_name
                     FROM tournament_matches tm
                     JOIN teams t1 ON tm.team1_id = t1.id
                     JOIN teams t2 ON tm.team2_id = t2.id
                     LEFT JOIN teams tw ON tm.winner_team_id = tw.id
                     WHERE tm.id = ?
                 `);
                 // FIX: Added .bind() here
                 const { results } = await stmt.bind(tournamentMatchId).all<TournamentMatch>();

                 if (results && results.length > 0) {
                     return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     return new Response(JSON.stringify({ error: "Tournament match not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("D1 get tournament match error:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: "Failed to retrieve tournament match", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         }
         // --- Update Tournament Match ---
         else if (request.method === 'PUT' && pathParts.length === 4 && isSingleMatchEndpoint) {
             try {
                 // Allow updating round, number, team IDs, player order, scheduled time
                 const updates = await request.json<Partial<Omit<TournamentMatch, 'id' | 'match_do_id' | 'status' | 'winner_team_id' | 'created_at' | 'team1_code' | 'team1_name' | 'team2_code' | 'team2_name' | 'winner_team_code' | 'winner_team_name'>>>();
                 // Prevent changing status or winner_team_id via this endpoint, use specific actions
                 // delete updates.status;
                 // delete updates.winner_team_id;

                 // Optional: Validate team IDs if provided in updates
                 if (updates.team1_id !== undefined && updates.team1_id !== null) { // Check for undefined and null
                     const teamExists = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(updates.team1_id).first();
                     if (!teamExists) {
                          return new Response(JSON.stringify({ error: `Team1 ID '${updates.team1_id}' not found.` }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                 }
                  if (updates.team2_id !== undefined && updates.team2_id !== null) { // Check for undefined and null
                     const teamExists = await env.DB.prepare("SELECT id FROM teams WHERE id = ?").bind(updates.team2_id).first();
                     if (!teamExists) {
                          return new Response(JSON.stringify({ error: `Team2 ID '${updates.team2_id}' not found.` }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                 }
                 // Check if team IDs are the same if both are provided
                 if (updates.team1_id !== undefined && updates.team1_id !== null && updates.team2_id !== undefined && updates.team2_id !== null && updates.team1_id === updates.team2_id) {
                      return new Response(JSON.stringify({ error: "队伍A和队伍B不能是同一支队伍。" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }


                 const stmt = env.DB.prepare(
                     `UPDATE tournament_matches SET
                        tournament_round = COALESCE(?, tournament_round),
                        match_number_in_round = COALESCE(?, match_number_in_round),
                        team1_id = COALESCE(?, team1_id),
                        team2_id = COALESCE(?, team2_id),
                        team1_player_order = COALESCE(?, team1_player_order),
                        team2_player_order = COALESCE(?, team2_player_order),
                        scheduled_time = COALESCE(?, scheduled_time)
                      WHERE id = ?`
                 );
                 const result = await stmt.bind(
                     updates.tournament_round,
                     updates.match_number_in_round,
                     updates.team1_id,
                     updates.team2_id,
                     updates.team1_player_order,
                     updates.team2_player_order,
                     updates.scheduled_time,
                     tournamentMatchId
                 ).run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Tournament match not found or no changes made." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     // Fetch the updated match with team details
                     const updatedMatchStmt = env.DB.prepare(`
                         SELECT
                             tm.*,
                             t1.code AS team1_code, t1.name AS team1_name,
                             t2.code AS team2_code, t2.name AS team2_name,
                             tw.code AS winner_team_code, tw.name AS winner_team_name
                         FROM tournament_matches tm
                         JOIN teams t1 ON tm.team1_id = t1.id
                         JOIN teams t2 ON tm.team2_id = t2.id
                         LEFT JOIN teams tw ON tm.winner_team_id = tw.id
                         WHERE tm.id = ?
                     `);
                      const { results: updatedResults } = await updatedMatchStmt.bind(tournamentMatchId).all<TournamentMatch>();
                      const updatedMatch = updatedResults && updatedResults.length > 0 ? updatedResults[0] : null;

                     return new Response(JSON.stringify({ success: true, message: "Tournament match updated.", tournamentMatch: updatedMatch }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 update tournament match error:", result.error);
                      if (result.error?.includes("UNIQUE constraint failed: tournament_matches.tournament_round, tournament_matches.match_number_in_round")) {
                         return new Response(JSON.stringify({ error: `Match number ${updates.match_number_in_round} in round '${updates.tournament_round}' already exists.` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                      }
                     return new Response(JSON.stringify({ error: "Failed to update tournament match", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during update tournament match:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception updating tournament match: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         }
         // --- Delete Tournament Match ---
         else if (request.method === 'DELETE' && pathParts.length === 4 && isSingleMatchEndpoint) {
             // Delete Tournament Match
             try {
                 // Prevent deleting if it's currently live or has archived data linked
                 const matchEntry = await env.DB.prepare("SELECT match_do_id FROM tournament_matches WHERE id = ?").bind(tournamentMatchId).first<{ match_do_id: string | null }>();
                 if (!matchEntry) {
                      return new Response(JSON.stringify({ success: false, message: "Tournament match not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                 }
                 if (matchEntry.match_do_id) {
                      // Check if the linked DO still exists and is not archived_in_d1
                      // This is complex. A simpler check is just if match_do_id is present.
                      // If match_do_id is present, assume it was started and might have archives.
                      // You might need a more robust check here depending on desired behavior.
                      return new Response(JSON.stringify({ error: "Cannot delete tournament match: It has been started or completed." }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }


                 const stmt = env.DB.prepare("DELETE FROM tournament_matches WHERE id = ?").bind(tournamentMatchId);
                 const result = await stmt.run();

                 if (result.success) {
                     if (result.meta.rows_affected === 0) {
                          return new Response(JSON.stringify({ success: false, message: "Tournament match not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                     }
                     return new Response(JSON.stringify({ success: true, message: "Tournament match deleted." }), { headers: { 'Content-Type': 'application/json' } });
                 } else {
                     console.error("D1 delete tournament match error:", result.error);
                     return new Response(JSON.stringify({ error: "Failed to delete tournament match", details: result.error }), { status: 500, headers: { 'Content-Type': 'application/json' } });
                 }
             } catch (e: any) {
                 console.error("Worker exception during delete tournament match:", e.stack); // Use e.stack
                 return new Response(JSON.stringify({ error: `Exception deleting tournament match: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
             }
         }
         // --- Start a Live Match from Schedule ---
         else if (url.pathname.endsWith('/start_live') && request.method === 'POST' && isSingleMatchEndpoint) {
            // tournamentMatchId is already parsed above

            try {
                // 1. Fetch the tournament match details and associated teams
                const matchStmt = env.DB.prepare(`
                    SELECT
                        tm.*,
                        t1.code AS team1_code, t1.name AS team1_name,
                        t2.code AS team2_code, t2.name AS team2_name
                    FROM tournament_matches tm
                    JOIN teams t1 ON tm.team1_id = t1.id
                    JOIN teams t2 ON tm.team2_id = t2.id
                    WHERE tm.id = ?
                `);
                // FIX: Added .bind() here
                const { results } = await matchStmt.bind(tournamentMatchId).all<TournamentMatch>();

                if (!results || results.length === 0) {
                    return new Response(JSON.stringify({ error: "Tournament match not found." }), { status: 404, headers: { 'Content-Type': 'application/json' } });
                }
                const tournamentMatch = results[0];

                // Prevent starting if already live or completed
                if (tournamentMatch.status === 'live') {
                     return new Response(JSON.stringify({ error: "Match is already live." }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                }
                 if (tournamentMatch.status === 'completed' || tournamentMatch.status === 'archived') { // 'archived' status is from old schema, handle it
                     return new Response(JSON.stringify({ error: "Match is already completed or archived." }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                }

                // 2. Fetch members for each team, ordered by ID
                const team1MembersStmt = env.DB.prepare("SELECT * FROM members WHERE team_code = ? ORDER BY id ASC").bind(tournamentMatch.team1_code);
                const team2MembersStmt = env.DB.prepare("SELECT * FROM members WHERE team_code = ? ORDER BY id ASC").bind(tournamentMatch.team2_code);

                const [{ results: team1Members }, { results: team2Members }] = await Promise.all([
                    team1MembersStmt.all<Member>(),
                    team2MembersStmt.all<Member>()
                ]);

                if (!team1Members || team1Members.length === 0 || !team2Members || team2Members.length === 0) {
                     return new Response(JSON.stringify({ error: "One or both teams have no members." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                }
                 // Optional: Enforce 3 members per team
                 if (team1Members.length < 3 || team2Members.length < 3) {
                      // return new Response(JSON.stringify({ error: "Both teams must have at least 3 members." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }


                // 3. Parse player order strings and get the ordered member IDs
                const team1PlayerOrderNumbers = parsePlayerOrder(tournamentMatch.team1_player_order);
                const team2PlayerOrderNumbers = parsePlayerOrder(tournamentMatch.team2_player_order);

                // Map 1-based order numbers to actual member IDs from the fetched and sorted member lists
                const team1PlayerOrderIds = team1PlayerOrderNumbers
                    .map(orderNum => team1Members[orderNum - 1]?.id) // Get member ID using 0-based index (orderNum - 1)
                    .filter((id): id is number => id !== undefined); // Filter out undefined if order number was invalid

                const team2PlayerOrderIds = team2PlayerOrderNumbers
                     .map(orderNum => team2Members[orderNum - 1]?.id)
                     .filter((id): id is number => id !== undefined);

                 // Fallback to default order (by member ID) if parsing failed or resulted in empty arrays
                 if (team1PlayerOrderIds.length === 0 && team1Members.length > 0) {
                     console.warn(`Worker: Invalid player order for team1 (${tournamentMatch.team1_code}). Using default order by member ID.`);
                     team1PlayerOrderIds.push(...team1Members.map(m => m.id)); // Use all member IDs in fetched order
                 }
                  if (team2PlayerOrderIds.length === 0 && team2Members.length > 0) {
                     console.warn(`Worker: Invalid player order for team2 (${tournamentMatch.team2_code}). Using default order by member ID.`);
                     team2PlayerOrderIds.push(...team2Members.map(m => m.id)); // Use all member IDs in fetched order
                 }

                 if (team1PlayerOrderIds.length === 0 || team2PlayerOrderIds.length === 0) {
                      return new Response(JSON.stringify({ error: "Could not determine valid player order for one or both teams. Ensure teams have members and player order is valid (e.g., '1,2,3')." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }


                // Prepare data for DO initialization
                const scheduleDataForDO: MatchScheduleData = { // Use the new type
                    tournamentMatchId: tournamentMatch.id,
                    team1_name: tournamentMatch.team1_name || '队伍A', // Provide default names
                    team2_name: tournamentMatch.team2_name || '队伍B',
                    team1_members: team1Members, // Pass full member objects
                    team2_members: team2Members, // Pass full member objects
                    team1_player_order_ids: team1PlayerOrderIds, // Pass ordered member IDs
                    team2_player_order_ids: team2PlayerOrderIds, // Pass ordered member IDs
                    round_name: tournamentMatch.tournament_round, // Pass round name
                    match_number_in_round: tournamentMatch.match_number_in_round, // Pass match number
                };

                // 4. Get or create Durable Object instance for THIS scheduled match
                // FIX: Use tournamentMatchId to derive the DO ID
                const matchDOId = env.MATCH_DO.idFromName(tournamentMatchId.toString());
                const matchStub = env.MATCH_DO.get(matchDOId);


                // Call the DO's internal initialization endpoint
                const initResponse = await matchStub.fetch(new Request(url.origin + "/internal/initialize-from-schedule", {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(scheduleDataForDO),
                }));

                const initResult = await initResponse.json();

                if (!initResponse.ok || !initResult.success) {
                    console.error("DO initialization failed:", initResult.message);
                    return new Response(JSON.stringify({ error: "Failed to initialize live match in DO", details: initResult.message }), { status: initResponse.status });
                }

                // 5. Update the tournament_matches entry in D1
                const updateStmt = env.DB.prepare("UPDATE tournament_matches SET status = ?, match_do_id = ? WHERE id = ?");
                // FIX: Use the newly created matchDOId.toString()
                const updateResult = await updateStmt.bind('live', matchDOId.toString(), tournamentMatchId).run();

                if (!updateResult.success) {
                     console.error(`Worker failed to update tournament_matches entry ${tournamentMatchId} status to 'live':`, updateResult.error);
                     // This is a partial failure. The DO is initialized, but the D1 schedule isn't updated.
                     // Decide how to handle this. For now, return success based on DO init, but log the D1 error.
                     // A more robust system might require rollback or manual intervention.
                } else {
                     console.log(`Worker updated tournament_matches entry ${tournamentMatchId} status to 'live' and linked DO ID.`);
                }

                // FIX: Return the newly created matchDOId
                return new Response(JSON.stringify({ success: true, message: "Live match started from schedule.", matchDOId: matchDOId.toString(), tournamentMatchId: tournamentMatch.id }), { headers: { 'Content-Type': 'application/json' } });

            } catch (e: any) {
                console.error("Worker exception during start_live:", e.stack); // Use e.stack
                return new Response(JSON.stringify({ error: `Exception starting live match: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
         }
    }

    // --- Bulk Import Tournament Matches Endpoint ---
     if (url.pathname === '/api/tournament_matches/bulk' && request.method === 'POST') {
        try {
            const matchesData = await request.json<BulkTournamentMatchRow[]>(); // Use the specific bulk type
            if (!Array.isArray(matchesData)) {
                 return new Response(JSON.stringify({ error: "Invalid payload: Expected an array of tournament matches." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }

            // Basic validation for each row and collect team IDs
            const teamIds = new Set<number>();
            const roundMatchNumbers = new Set<string>(); // To check for duplicates in input
            for (const match of matchesData) {
                 // Validate required fields and types from CSV strings
                 const matchNumber = parseInt(match.match_number_in_round, 10);
                 const team1Id = parseInt(match.team1_id, 10);
                 const team2Id = parseInt(match.team2_id, 10);

                 if (!match.tournament_round || isNaN(matchNumber) || matchNumber < 1 || isNaN(team1Id) || isNaN(team2Id)) {
                      return new Response(JSON.stringify({ error: `Invalid match data found in input: round='${match.tournament_round}', number='${match.match_number_in_round}', team1_id='${match.team1_id}', team2_id='${match.team2_id}'. Round, number (>=1), team1_id, team2_id are required and must be valid numbers.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 if (team1Id === team2Id) {
                      return new Response(JSON.stringify({ error: `Invalid match data found in input: Teams cannot be the same for round='${match.tournament_round}', number='${match.match_number_in_round}'.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 const key = `${match.tournament_round}-${matchNumber}`;
                 if (roundMatchNumbers.has(key)) {
                      return new Response(JSON.stringify({ error: `Duplicate match found in input data: Round '${match.tournament_round}', Number ${matchNumber}.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
                 roundMatchNumbers.add(key);

                 teamIds.add(team1Id);
                 teamIds.add(team2Id);
            }

            // Check if all team IDs exist in the database
            if (teamIds.size > 0) {
                 const teamIdArray = Array.from(teamIds);
                 const existingTeamsStmt = env.DB.prepare(`SELECT id FROM teams WHERE id IN (${teamIdArray.map(() => '?').join(',')})`).bind(...teamIdArray);
                 const { results: existingTeams } = await existingTeamsStmt.all<{ id: number }>();
                 const existingTeamIds = new Set(existingTeams?.map(t => t.id) || []);

                 const invalidTeamIds = teamIdArray.filter(id => !existingTeamIds.has(id));
                 if (invalidTeamIds.length > 0) {
                      return new Response(JSON.stringify({ error: `Bulk import failed: Team IDs not found: ${invalidTeamIds.join(', ')}. Please ensure teams exist before importing matches.` }), { status: 400, headers: { 'Content-Type': 'application/json' } });
                 }
            }

             // Check for existing round+number combinations in the database
             if (roundMatchNumbers.size > 0) {
                 const existingMatchesStmt = env.DB.prepare(`SELECT tournament_round, match_number_in_round FROM tournament_matches WHERE ${Array.from(roundMatchNumbers).map(() => "(tournament_round = ? AND match_number_in_round = ?)").join(" OR ")}`);
                 const bindParams: (string | number)[] = [];
                 Array.from(roundMatchNumbers).forEach(key => {
                     const [round, number] = key.split('-');
                     bindParams.push(round, parseInt(number, 10));
                 });
                 const { results: existingMatches } = await existingMatchesStmt.bind(...bindParams).all<{ tournament_round: string, match_number_in_round: number }>();

                 if (existingMatches && existingMatches.length > 0) {
                      const existingKeys = existingMatches.map(m => `${m.tournament_round}-${m.match_number_in_round}`);
                      return new Response(JSON.stringify({ error: `Bulk import failed: Matches already exist: ${existingKeys.join(', ')}. Please remove existing matches.` }), { status: 409, headers: { 'Content-Type': 'application/json' } });
                 }
             }


            const stmt = env.DB.prepare("INSERT INTO tournament_matches (tournament_round, match_number_in_round, team1_id, team2_id, team1_player_order, team2_player_order, scheduled_time, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            const now = new Date().toISOString();
            const insertBatch = matchesData.map(match => {
                 return stmt.bind(
                     match.tournament_round,
                     parseInt(match.match_number_in_round, 10), // Parse to number
                     parseInt(match.team1_id, 10), // Parse to number
                     parseInt(match.team2_id, 10), // Parse to number
                     match.team1_player_order || null,
                     match.team2_player_order || null,
                     match.scheduled_time || null,
                     now,
                     'scheduled' // Default status for bulk import
                 );
            });

            // Use batch for efficiency
            const results = await env.DB.batch(insertBatch);

            // Check results for errors (batch might succeed partially, though we pre-checked)
            const errors = results.filter(r => !r.success);
            const successCount = results.length - errors.length;

            if (errors.length > 0) {
                 console.error("D1 bulk create tournament matches errors:", errors);
                 return new Response(JSON.stringify({
                     success: successCount > 0, // Consider success if at least one row inserted
                     message: `Bulk import completed with errors. Successfully imported ${successCount} matches. Failed to import ${errors.length} matches.`,
                     errors: errors.map(e => e.error)
                 }), { status: successCount > 0 ? 207 : 500, headers: { 'Content-Type': 'application/json' } }); // 207 Multi-Status
            }

            return new Response(JSON.stringify({ success: true, message: `Bulk import successful. ${successCount} tournament matches imported.` }), { status: 201, headers: { 'Content-Type': 'application/json' } });

        } catch (e: any) {
            console.error("Worker exception during bulk create tournament matches:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: `Exception during bulk import tournament matches: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // Endpoint to get archived round data from D1 for a specific match DO ID
    // This endpoint is used by the frontend to display archived rounds for a finished match
    if (url.pathname.startsWith('/api/archived_rounds/') && request.method === 'GET') {
        const pathParts = url.pathname.split('/');
        // Expecting /api/archived_rounds/{match_do_id}
        const matchDOId = pathParts[3];

        if (!matchDOId) {
             return new Response(JSON.stringify({ error: "Match DO ID missing in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }

        try {
            // Fetch all rounds for the specified match DO, ordered by round number
            const stmt = env.DB.prepare("SELECT * FROM round_archives WHERE match_do_id = ? ORDER BY round_number ASC");
            const { results } = await stmt.bind(matchDOId).all<RoundArchive>();

            return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
        } catch (e: any) {
            console.error("Worker D1 round query error:", e.stack); // Use e.stack
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
                // Fetch the updated record to return it
                const fetchUpdatedStmt = env.DB.prepare("SELECT * FROM round_archives WHERE id = ?").bind(roundArchiveId);
                const { results: updatedResults } = await fetchUpdatedStmt.all<RoundArchive>();

                return new Response(JSON.stringify({ success: true, message: "Archived round updated.", updatedRecord: updatedResults ? updatedResults[0] : null }), { headers: { 'Content-Type': 'application/json' } });
            } else {
                console.error(`Worker: Failed to update archived round ${roundArchiveId} in D1:`, result.error);
                return new Response(JSON.stringify({ success: false, message: `Failed to update archived round: ${result.error}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }

        } catch (e: any) {
            console.error(`Worker: Exception during D1 archived round update:`, e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: `Exception during archived round update: ${e.message}` }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // Endpoint to get archived match summary data from D1
    // This endpoint is used to list all finished matches
    if (url.pathname === '/api/archived_matches' && request.method === 'GET') {
        try {
            // Fetch all archived match summaries (consider pagination for large lists)
            const stmt = env.DB.prepare("SELECT id, match_do_id, tournament_match_id, match_name, final_round, team_a_name, team_b_name, team_a_score, team_b_score, winner_team_name, status, archived_at FROM matches_archive ORDER BY archived_at DESC LIMIT 50");
            const { results } = await stmt.all<MatchArchiveSummary>();
            return new Response(JSON.stringify(results), { headers: { 'Content-Type': 'application/json' } });
        } catch (e: any) {
            console.error("Worker D1 match summary query error:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: "Failed to retrieve archived match summaries", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }

     // Endpoint to get a specific archived match summary by DO ID
     if (url.pathname.startsWith('/api/archived_matches/') && request.method === 'GET') {
        const pathParts = url.pathname.split('/');
        // Expecting /api/archived_matches/{match_do_id}
        const matchDOId = pathParts[3];

        if (!matchDOId) {
             return new Response(JSON.stringify({ error: "Match DO ID missing in path" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }

        try {
            // Fetch a specific archived match summary by DO ID
            const stmt = env.DB.prepare("SELECT * FROM matches_archive WHERE match_do_id = ?");
            const { results } = await stmt.bind(matchDOId).all<MatchArchiveSummary>();
            if (results && results.length > 0) {
                return new Response(JSON.stringify(results[0]), { headers: { 'Content-Type': 'application/json' } });
            } else {
                return new Response(JSON.stringify({ error: "Archived match summary not found" }), { status: 404, headers: { 'Content-Type': 'application/json' } });
            }
        } catch (e: any) {
            console.error("Worker D1 match summary query error:", e.stack); // Use e.stack
            return new Response(JSON.stringify({ error: "Failed to retrieve archived match summary", details: e.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
        }
    }


    // Fallback for other requests or static assets (if any served by this worker)
    return new Response('Not found. API endpoints are /api/match/* (singleton DO), /api/teams, /api/teams/:id, /api/teams/bulk, /api/members, /api/members/:id, /api/teams/:team_code/members, /api/members/bulk, /api/tournament_matches, /api/tournament_matches/:id, /api/tournament_matches/bulk, /api/tournament_matches/:id/start_live, /api/archived_rounds/{match_do_id}, /api/archived_rounds/:id (PUT), /api/archived_matches, /api/archived_matches/{match_do_id}', { status: 404 });
  },
};

// Export the Durable Object class
export { MatchDO };
