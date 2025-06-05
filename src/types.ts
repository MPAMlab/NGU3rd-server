// src/types.ts

// Represents the environment variables and bindings for the Worker
export interface Env {
    MATCH_DO: DurableObjectNamespace; // Durable Object binding
    DB: D1Database; // D1 database binding
    AVATAR_BUCKET: R2Bucket; // R2 bucket for avatars
    SONG_COVER_BUCKET: R2Bucket; // R2 bucket for song covers (Keep this binding)

    // Kinde Secrets (from Cloudflare Pages/Worker Environment Variables)
    KINDE_CLIENT_ID: string;
    KINDE_CLIENT_SECRET: string;
    KINDE_ISSUER_URL: string; // e.g., https://YOUR_KINDE_DOMAIN.kinde.com
    KINDE_REDIRECT_URI: string; // e.g., https://your-app.pages.dev/callback
    LOGOUT_REDIRECT_TARGET_URL: string; // e.g., https://your-app.pages.dev/

    // Other Secrets
    ADMIN_API_KEY?: string; // Optional: If you still need a fallback admin key, otherwise remove
    R2_PUBLIC_BUCKET_URL: string; // <-- ADD THIS: The base URL for your R2 bucket (e.g., https://pub-xxxxxxxxxxxx.r2.dev)
}

// --- 固定表相关类型 (members, teams) ---
export interface Team {
    id: number;
    code: string;
    name: string;
    created_at?: number | null; // Unix timestamp
    current_health?: number | null;
    has_revive_mirror?: number | null;
    status?: string | null;
}

export interface Member {
    id: number;
    team_code: string;
    color?: string | null;
    job?: string | null;
    maimai_id?: string | null;
    nickname: string;
    qq_number?: string | null;
    avatar_url?: string | null;
    joined_at?: number | null; // Unix timestamp
    updated_at?: number | null; // Unix timestamp
    kinde_user_id: string | null; // <-- CONFIRMED: Kinde User ID
    is_admin: number | null; // <-- CONFIRMED: 0 or 1
}

// Basic Kinde User Info (from ID token payload, returned by backend callback)
export interface KindeUser {
    id: string; // Kinde User ID (sub claim)
    email?: string;
    name?: string; // Or other name claims like given_name, family_name
    // Add other claims you might need from the ID token
}


// --- 歌曲相关类型 ---
export interface SongLevel {
    B?: string;
    A?: string;
    E?: string;
    M?: string;
    R?: string;
}

// REMOVED: ImportedSongItem (Assuming admin import is handled elsewhere or differently now)
// REMOVED: ImportSongsPayload (Assuming admin import is handled elsewhere or differently now)

export interface Song {
    id: number;
    title: string;
    category?: string | null;
    bpm?: string | null;
    levels_json?: string | null;
    type?: string | null;
    cover_filename?: string | null;
    source_data_version?: string | null;
    created_at?: string;

    // Frontend convenience fields (populated by Worker)
    parsedLevels?: SongLevel;
    fullCoverUrl?: string;
}

export interface MemberSongPreference {
    id?: number;
    member_id: number;
    tournament_stage: string;
    song_id: number;
    selected_difficulty: string;
    created_at?: string;

    // Denormalized song info for display (fetched via JOIN or separate query)
    song_title?: string;
    cover_filename?: string;
    fullCoverUrl?: string;
    parsedLevels?: SongLevel;
}

// Payload for saving a member's song preference (POST /api/member_song_preferences)
export interface SaveMemberSongPreferencePayload {
    member_id: number;
    tournament_stage: string;
    song_id: number;
    selected_difficulty: string;
}


// --- 比赛核心类型 (与之前相同) ---
export interface MatchSong {
    song_id: number;
    song_title: string;
    song_difficulty: string;
    song_element?: 'fire' | 'wood' | 'water' | null;
    cover_filename?: string | null;
    bpm?: string | null;
    fullCoverUrl?: string;
    picker_member_id: number;
    picker_team_id: number;
    is_tiebreaker_song?: boolean;
    status: 'pending' | 'ongoing' | 'completed';
    teamA_player_id?: number;
    teamB_player_id?: number;
    teamA_percentage?: number;
    teamB_percentage?: number;
    teamA_damage_dealt?: number;
    teamB_damage_dealt?: number;
    teamA_effect_value?: number;
    teamB_effect_value?: number;
    teamA_health_after?: number;
    teamB_health_after?: number;
    teamA_mirror_triggered?: boolean;
    teamB_mirror_triggered?: boolean;
}

export interface TournamentMatch {
    id: number;
    round_name: string;
    team1_id: number;
    team2_id: number;
    status: 'scheduled' | 'pending_song_confirmation' | 'ready_to_start' | 'live' | 'completed' | 'archived';
    winner_team_id?: number | null;
    match_do_id?: string | null;
    scheduled_time?: string | null;
    current_match_song_index?: number; // Added this based on DO state
    team1_player_order_json?: string; // Raw JSON from D1
    team2_player_order_json?: string; // Raw JSON from D1
    match_song_list_json?: string;    // Raw JSON from D1
    team1_player_order?: number[] | null; // Parsed by Worker/Frontend
    team2_player_order?: number[] | null; // Parsed by Worker/Frontend
    match_song_list?: MatchSong[] | null; // Parsed by Worker/Frontend
    team1_name?: string;
    team2_name?: string;
    winner_team_name?: string;
    team1_code?: string;
    team2_code?: string;
    winner_team_code?: string;
    final_score_team1?: number | null;
    final_score_team2?: number | null;
    created_at: string;
    updated_at?: string;
}

// Payload for creating a new Tournament Match (POST /api/tournament_matches)
export interface CreateTournamentMatchPayload {
    round_name: string;
    team1_id: number | null;
    team2_id: number | null;
    scheduled_time?: string | null;
}

// Payload for Staff to confirm match setup (PUT /api/tournament_matches/:id/confirm_setup)
export interface ConfirmMatchSetupPayload {
    team1_player_order: number[];
    team2_player_order: number[];
    match_song_list: MatchSong[];
}

export interface MatchState {
    match_do_id: string; // Actual DO hex ID
    tournament_match_id: number;
    status: 'pending_scores' | 'round_finished' | 'team_A_wins' | 'team_B_wins' | 'draw_pending_resolution' | 'tiebreaker_pending_song' | 'archived';
    round_name: string;
    current_match_song_index: number;
    teamA_id: number;
    teamB_id: number;
    teamA_name: string;
    teamB_name: string;
    teamA_score: number;
    teamB_score: number;
    teamA_player_order_ids: number[];
    teamB_player_order_ids: number[];
    teamA_current_player_id: number | null;
    teamB_current_player_id: number | null;
    teamA_current_player_nickname?: string;
    teamB_current_player_nickname?: string;
    teamA_current_player_profession?: string | null;
    teamB_current_player_profession?: string | null; // Added missing property
    teamA_mirror_available: boolean;
    teamB_mirror_available: boolean;
    match_song_list: MatchSong[];
    current_song: MatchSong | null;
    roundSummary: RoundSummary | null;
    // teamA_members and teamB_members are passed during initialization but not typically part of the broadcasted state
    teamA_members?: Member[];
    teamB_members?: Member[];
}

// Payload for initializing DO from D1 TournamentMatch data (Internal to Worker/DO)
export interface MatchScheduleData {
    tournamentMatchId: number;
    round_name: string;
    team1_id: number;
    team2_id: number;
    team1_name: string;
    team2_name: string;
    team1_members: Member[];
    team2_members: Member[];
    team1_player_order_ids: number[];
    team2_player_order_ids: number[];
    match_song_list: MatchSong[];
}


export interface CalculateRoundPayload {
    teamA_percentage: number;
    teamB_percentage: number;
    teamA_effect_value?: number;
    teamB_effect_value?: number;
}

export interface ResolveDrawPayload {
    winner: 'teamA' | 'teamB';
}

export interface SelectTiebreakerSongPayload {
    song_id: number;
    selected_difficulty: string;
}

export interface RoundSummary {
    round_number_in_match: number;
    song_id: number;
    song_title: string;
    selected_difficulty: string;
    teamA_player_id: number;
    teamB_player_id: number;
    teamA_player_nickname: string;
    teamB_player_nickname: string;
    teamA_percentage: number;
    teamB_percentage: number;
    teamA_effect_value_applied: number;
    teamB_effect_value_applied: number;
    teamA_damage_digits: number[];
    teamB_damage_digits: number[];
    teamA_base_damage: number;
    teamB_base_damage: number;
    teamA_profession?: string | null;
    teamB_profession?: string | null;
    teamA_profession_effect_applied?: string;
    teamB_profession_effect_applied?: string;
    teamA_modified_damage_to_B: number;
    teamB_modified_damage_to_A: number;
    teamA_health_before_round: number;
    teamB_health_before_round: number;
    teamA_mirror_triggered: boolean;
    teamB_mirror_triggered: boolean;
    teamA_mirror_effect_applied?: string;
    teamB_mirror_effect_applied?: string;
    teamA_supporter_base_skill_heal?: number;
    teamB_supporter_base_skill_heal?: number;
    teamA_supporter_mirror_bonus_heal?: number;
    teamB_supporter_mirror_bonus_heal?: number;
    teamA_final_damage_dealt: number;
    teamB_final_damage_dealt: number;
    teamA_health_change: number;
    teamB_health_change: number;
    teamA_health_after: number;
    teamB_health_after: number;
    is_tiebreaker_song?: boolean;
    log?: string[];
}

export interface MatchHistoryRound {
    id: number; // match_rounds_history PK
    tournament_match_id: number;
    match_do_id: string; // Actual DO hex ID
    round_number_in_match: number;
    song_id: number | null;
    selected_difficulty: string | null;
    picker_team_id: number | null;
    picker_member_id: number | null;
    team1_member_id: number | null;
    team2_member_id: number | null;
    team1_percentage: number | null;
    team2_percentage: number | null;
    team1_damage_dealt: number | null;
    team2_damage_dealt: number | null;
    team1_health_change: number | null;
    team2_health_change: number | null;
    team1_health_before: number | null;
    team2_health_before: number | null;
    team1_health_after: number | null;
    team2_health_after: number | null;
    team1_mirror_triggered: number | null; // D1 stores 0/1
    team2_mirror_triggered: number | null; // D1 stores 0/1
    team1_effect_value: number | null;
    team2_effect_value: number | null;
    is_tiebreaker_song: number | null; // D1 stores 0/1
    recorded_at: string;
    round_summary_json: string | null; // Raw JSON string from D1

    // Denormalized fields from JOINs
    song_title?: string | null;
    cover_filename?: string | null;
    picker_team_name?: string | null;
    picker_member_nickname?: string | null;
    team1_member_nickname?: string | null;
    team2_member_nickname?: string | null;

    // Frontend convenience
    round_summary?: RoundSummary | null; // Parsed RoundSummary
    fullCoverUrl?: string; // Constructed R2 URL
}

export interface MatchHistoryMatch {
    id: number; // tournament_matches PK
    round_name: string;
    scheduled_time: string | null;
    status: 'completed' | 'archived'; // History only shows these statuses
    final_score_team1: number | null;
    final_score_team2: number | null;

    // Denormalized fields from JOINs
    team1_name?: string;
    team2_name?: string;
    winner_team_name?: string;

    // Associated rounds
    rounds: MatchHistoryRound[];
}

// Generic API Response Wrapper (used by both frontend and backend)
export interface ApiResponse<T = any> {
    success: boolean;
    data?: T;
    error?: string;
    message?: string;
}

export type InternalProfession = 'attacker' | 'defender' | 'supporter' | null;


// --- NEW TYPES FOR PAGINATION AND SONG FILTERS (from store.ts) ---
// Move these from store.ts to here for shared use

export interface PaginationInfo {
    currentPage: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
}

// Specific response data structure for GET /api/songs
export interface SongsApiResponseData {
    songs: Song[]; // Array of songs for the current page
    pagination: PaginationInfo; // Pagination metadata
}

// Specific response data structure for GET /api/songs/filters
export interface SongFiltersApiResponseData {
    categories: string[];
    types: string[];
}

// Represents a player's song selections and order for a specific match (matches the DB table)
export interface MatchPlayerSelection {
    id?: number; // D1 PK
    tournament_match_id: number; // FK to tournament_matches
    member_id: number; // FK to members
    team_id: number; // FK to teams
    song1_id: number; // FK to songs
    song1_difficulty: string; // e.g., 'M', 'E' (Difficulty key)
    song2_id: number; // FK to songs
    song2_difficulty: string; // e.g., 'M', 'E' (Difficulty key)
    selected_order_index: number; // 0-based index (0 for 1st, 1 for 2nd, etc.)
    created_at?: string;
    updated_at?: string;
}

// Payload for saving a player's match selection (POST /api/member/match-selection/:matchId)
export interface SaveMatchPlayerSelectionPayload {
    song1_id: number;
    song1_difficulty: string;
    song2_id: number;
    song2_difficulty: string;
    selected_order_index: number;
}

// Response data for fetching user's match selection view data (GET /api/member/match-selection/:matchId)
// This structure is what the backend sends to the frontend.
export interface FetchUserMatchSelectionData { // Renamed to match frontend expectation
    match: TournamentMatch; // Basic match info
    myTeam: Team;
    opponentTeam: Team;
    myTeamMembers: Member[]; // Full member list for user's team
    opponentTeamMembers: Member[]; // Full member list for opponent's team
    mySelection: MatchPlayerSelection | null; // User's existing selection
    // Occupied indices need member_id and nickname for frontend display
    occupiedOrderIndices: { team_id: number; selected_order_index: number; member_id: number; member_nickname?: string }[];
    availableOrderSlotsCount: number; // Total number of slots available per team (e.g., 3 for 3v3)
    // Note: allSongs is NOT included here, frontend fetches it separately
}

// Response data for checking selection status (GET /api/tournament_matches/:matchId/selection-status)
export interface MatchSelectionStatus { // Renamed to match frontend expectation
    matchId: number;
    isReadyToCompile: boolean; // True if all players have selected
    team1Status: {
        teamId: number;
        teamName: string;
        requiredSelections: number; // Number of players expected
        completedSelections: number; // Number of players who have selected
        missingMembers: { id: number; nickname: string }[]; // List of members missing selections
    };
    team2Status: {
        teamId: number;
        teamName: string;
        requiredSelections: number;
        completedSelections: number;
        missingMembers: { id: number; nickname: string }[];
    };
}

// Response data for compiling final match setup from player selections (POST /api/tournament_matches/:matchId/compile-setup)
export interface CompileMatchSetupResponse { // Renamed to match frontend expectation
    success: boolean;
    message: string;
    tournamentMatch?: TournamentMatch; // Optional: return the updated match
}