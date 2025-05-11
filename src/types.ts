// src/types.ts

// Represents the environment variables and bindings
export interface Env {
    MATCH_DO: DurableObjectNamespace;
    DB: D1Database;
    AVATAR_BUCKET: R2Bucket;
    SONG_COVER_BUCKET: R2Bucket; // **保留这个绑定**
}

// --- 固定表相关类型 (members, teams) ---
export interface Team {
    id: number;
    code: string;
    name: string;
    created_at?: number | null;
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
    joined_at?: number | null;
    updated_at?: number | null;
    kinde_user_id?: string | null;
    is_admin?: number | null;
}

// --- 歌曲相关类型 ---
export interface SongLevel {
    B?: string;
    A?: string;
    E?: string;
    M?: string;
    R?: string;
}

// REMOVED: ImportedSongItem (不再需要)
// REMOVED: ImportSongsPayload (不再需要)

export interface Song {
    id: number;
    title: string;
    category?: string | null;
    bpm?: string | null;
    levels_json?: string | null;
    type?: string | null;
    cover_filename?: string | null;
    source_data_version?: string | null; // 仍然可以用来标记数据来源或版本
    created_at?: string;

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

    song_title?: string;
    cover_filename?: string;
    fullCoverUrl?: string;
    parsedLevels?: SongLevel;
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
    current_match_song_index?: number;
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
    match_do_id: string;
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
    teamA_mirror_available: boolean;
    teamB_mirror_available: boolean;
    match_song_list: MatchSong[];
    current_song: MatchSong | null;
    roundSummary: RoundSummary | null;
    // teamA_members and teamB_members are passed during initialization but not typically part of the broadcasted state
    teamA_members?: Member[];
    teamB_members?: Member[];
}

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
    id: number;
    tournament_match_id: number;
    match_do_id: string;
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
    team1_mirror_triggered: number | null;
    team2_mirror_triggered: number | null;
    team1_effect_value: number | null;
    team2_effect_value: number | null;
    is_tiebreaker_song: number | null;
    recorded_at: string;
    round_summary_json: string | null;
    song_title?: string | null;
    cover_filename?: string | null;
    picker_team_name?: string | null;
    picker_member_nickname?: string | null;
    team1_member_nickname?: string | null;
    team2_member_nickname?: string | null;
    round_summary?: RoundSummary | null;
    fullCoverUrl?: string;
}

export interface MatchHistoryMatch {
    id: number;
    round_name: string;
    scheduled_time: string | null;
    status: 'completed' | 'archived';
    final_score_team1: number | null;
    final_score_team2: number | null;
    team1_name?: string;
    team2_name?: string;
    winner_team_name?: string;
    rounds: MatchHistoryRound[];
}

export interface ApiResponse<T = any> {
    success: boolean;
    data?: T;
    error?: string;
    message?: string;
}

export type InternalProfession = 'attacker' | 'defender' | 'supporter' | null;
