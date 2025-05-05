// src/handlers/public/settings.ts

import { Request, Env } from '../../types';
import { apiResponse, apiError } from '../../index'; // Assuming these are exported from index.ts

// Helper function to check collection status (copied from App 1 Worker)
async function isCollectionPaused(env: Env): Promise<boolean> {
    try {
        const setting = await env.DB.prepare('SELECT value FROM settings WHERE key = ? LIMIT 1')
            .bind('collection_paused')
            .first<{ value: string }>();
        // Return true if the setting exists and its value is 'true'
        return setting?.value === 'true';
    } catch (e) {
        console.error('Database error fetching collection_paused setting:', e);
        // Default to not paused on error, or handle as needed
        return false;
    }
}

// GET /api/settings (Public)
export async function handleGetSettings(request: Request, env: Env): Promise<Response> {
    console.log('Handling /api/settings request...');
    try {
        const paused = await isCollectionPaused(env);
        // Add other public settings here if needed
        return apiResponse({ collection_paused: paused }, 200);
    } catch (e) {
        console.error('Error fetching settings:', e);
        return apiError('Failed to fetch settings.', 500, e);
    }
}
