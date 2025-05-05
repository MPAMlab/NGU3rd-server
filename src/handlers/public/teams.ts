// src/handlers/public/teams.ts

import { Request, Env, Team, Member } from '../../types';
import { apiResponse, apiError } from '../../index'; // Assuming these are exported from index.ts

// GET /api/teams/:code (Public - No auth needed)
export async function handleGetTeamByCode(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split('/');
    // Expecting path like /api/teams/1234
    if (parts.length !== 4 || !parts[3]) {
        return apiError('Invalid API path. Use /api/teams/:code', 400);
    }
    const teamCode = parts[3];

    if (teamCode.length !== 4 || isNaN(parseInt(teamCode))) {
        return apiError('Invalid team code format.', 400);
    }

    console.log(`Handling /api/teams/${teamCode} request...`);

    try {
        // Fetch team and its members
        const teamResult = await env.DB.prepare('SELECT id, code, name, current_health, has_revive_mirror, status FROM teams WHERE code = ? LIMIT 1').bind(teamCode).first<Team>();

        if (!teamResult) {
            return apiError(`Team with code ${teamCode} not found.`, 404);
        }

        // Select member fields relevant for public display
        const membersResult = await env.DB.prepare(
            'SELECT id, team_code, color, job, maimai_id, nickname, avatar_url FROM members WHERE team_code = ? ORDER BY joined_at ASC'
        ).bind(teamCode).all<Pick<Member, 'id' | 'team_code' | 'color' | 'job' | 'maimai_id' | 'nickname' | 'avatar_url'>>(); // Use Pick for specific fields

        return apiResponse({
            success: true,
            team: teamResult,
            members: membersResult.results || []
        }, 200);

    } catch (e) {
        console.error(`Database error fetching team by code ${teamCode}:`, e);
        return apiError('Failed to fetch team information.', 500, e);
    }
}
