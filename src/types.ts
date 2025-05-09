// src/types.ts

// Represents the environment variables and bindings
export interface Env {
    MATCH_DO: DurableObjectNamespace;
    DB: D1Database;
    AVATAR_BUCKET: R2Bucket;
    SONG_JSON_BUCKET: R2Bucket; // Add binding for the R2 bucket holding the song JSON
    SONG_JSON_KEY: string; // Add binding for the key/filename of the song JSON file
    SONG_COVER_BUCKET: R2Bucket; // Add binding for the R2 bucket holding song covers
}

// --- 固定表相关类型 (members, teams) ---
// 假设 id 为 number, teamId 在 Member 中也为 number
export interface Team {
    id: number; // D1 auto-increment ID
    code: string; // 4-character manual ID (your team_code)
    name: string; // your team_name
    created_at?: number | null; // Unix timestamp (matching your table)
    // current_health, has_revive_mirror, status 这些是比赛中的状态，由DO管理
    // 如果 teams 表中保留了这些字段，它们可能代表默认值或全局状态，与比赛实时状态区分
    current_health?: number | null;
    has_revive_mirror?: number | null; // 0 or 1
    status?: string | null; // e.g., 'active', 'inactive'
}

export interface Member {
    id: number; // D1 auto-increment ID
    team_code: string; // FK to teams.code (Keeping team_code as per your constraint)
    // Note: If you need team ID (number) in frontend, you'll need to join teams table
    color?: string | null; // e.g., 'fire', 'wood', 'water' (assuming D1 stores these strings)
    job?: string | null; // e.g., "绝剑士", "矩盾手", "炼星师"
    maimai_id?: string | null;
    nickname: string; // The player name to display
    qq_number?: string | null;
    avatar_url?: string | null; // Storing filename or full URL? If filename, need R2 binding
    joined_at?: number | null; // Unix timestamp
    updated_at?: number | null; // Unix timestamp
    kinde_user_id?: string | null;
    is_admin?: number | null; // 0 or 1
}

// --- 歌曲相关类型 ---
export interface SongLevel { // 对应 R2 JSON "等级"
    B?: string;
    A?: string;
    E?: string;
    M?: string;
    R?: string;
}

export interface SongFromR2 { // 对应 R2 JSON "曲目列表" 中的一项
    分类: string;
    曲名: string;
    BPM: string;
    等级: SongLevel;
    类型: string;
    封面: string; // 文件名
}

export interface R2SongList { // 对应整个 R2 JSON 文件结构
    data: string; // Version string
    曲目列表: SongFromR2[];
}


export interface Song { // 对应 D1 songs 表，也是前端主要使用的歌曲类型
    id: number;
    title: string;
    category?: string | null;
    bpm?: string | null;
    levels_json?: string | null; // Storing JSON string of SongLevel
    type?: string | null;
    cover_filename?: string | null;
    source_data_version?: string | null;
    created_at?: string;

    // Frontend convenience fields (populated by Worker/DO)
    parsedLevels?: SongLevel; // Parsed levels_json
    fullCoverUrl?: string; // Constructed R2 URL
}

// 用户为特定赛段选择的歌曲偏好 (对应 member_song_preferences 表)
export interface MemberSongPreference {
    id?: number; // D1 PK
    member_id: number;
    tournament_stage: string; // e.g., '初赛', '复赛'
    song_id: number;
    selected_difficulty: string; // e.g., 'M', 'E'
    created_at?: string;

    // Denormalized song info for display (fetched via JOIN or separate query)
    song_title?: string;
    cover_filename?: string;
    fullCoverUrl?: string;
    parsedLevels?: SongLevel;
}

// --- 比赛核心类型 ---

// 代表比赛歌单中的一首歌及其相关信息 (存储在 tournament_matches.match_song_list_json)
export interface MatchSong {
    song_id: number;
    song_title: string; // Denormalized
    song_difficulty: string; // 实际比赛选择的难度，例如 'M 13' (包含等级和难度值)
    song_element?: 'fire' | 'wood' | 'water' | null; // Denormalized (assuming color maps to element)
    cover_filename?: string | null; // Denormalized
    bpm?: string | null; // Denormalized
    fullCoverUrl?: string; // Denormalized

    // 记录这首歌的来源
    picker_member_id: number; // 选这首歌的选手 ID
    picker_team_id: number;   // 选这首歌的选手所属队伍 ID
    is_tiebreaker_song?: boolean; // 是否为加时赛歌曲

    status: 'pending' | 'ongoing' | 'completed'; // 在这场比赛中的状态

    // 比赛中这首歌的结果 (当 status 为 'completed' 时，DO 会填充这些)
    teamA_player_id?: number; // Team A 出战这首歌的选手 ID
    teamB_player_id?: number; // Team B 出战这首歌的选手 ID
    teamA_percentage?: number; // 完整百分比
    teamB_percentage?: number; // 完整百分比
    teamA_damage_dealt?: number;
    teamB_damage_dealt?: number;
    teamA_effect_value?: number; // 小分调整
    teamB_effect_value?: number;
    teamA_health_after?: number; // 本轮结束后血量
    teamB_health_after?: number; // 本轮结束后血量
    teamA_mirror_triggered?: boolean;
    teamB_mirror_triggered?: boolean;
}

// 赛程表条目 (对应 tournament_matches 表)
export interface TournamentMatch {
    id: number;
    round_name: string; // e.g., '初赛 - 第1轮'
    team1_id: number;
    team2_id: number;
    status: 'scheduled' | 'pending_song_confirmation' | 'ready_to_start' | 'live' | 'completed' | 'archived';
    winner_team_id?: number | null;
    match_do_id?: string | null;
    scheduled_time?: string | null;
    current_match_song_index?: number; // 当前打到歌单第几首 (0-based)

    // 从 D1 JSON 字段解析或由 API 组装
    team1_player_order?: number[] | null; // member_id 数组
    team2_player_order?: number[] | null; // member_id 数组
    match_song_list?: MatchSong[] | null; // 这场比赛的歌单

    // Denormalized for display convenience in lists (fetched via JOIN)
    team1_name?: string;
    team2_name?: string;
    winner_team_name?: string;

    // 比赛结束后的最终结果 (从 D1 final_score_... 读取)
    final_score_team1?: number | null;
    final_score_team2?: number | null;
}

// DO 的实时状态 (WebSocket 推送的内容)
export interface MatchState {
    match_do_id: string;
    tournament_match_id: number; // 关联的 D1 tournament_matches.id

    // DO 内部状态，反映当前比赛阶段
    status: 'pending_scores' | 'round_finished' | 'team_A_wins' | 'team_B_wins' | 'draw_pending_resolution' | 'tiebreaker_pending_song' | 'archived';
    // 'pending_scores': 等待当前歌曲的双方成绩
    // 'round_finished': 当前歌曲成绩已计算，等待下一首或比赛结束
    // 'tiebreaker_pending_song': 标准轮次打平，等待 Staff 选择加时赛歌曲
    // 'archived': 比赛已归档

    round_name: string; // e.g., '初赛 - 第1轮' (从 TournamentMatch 带过来)
    current_match_song_index: number; // 当前歌单的索引 (0-based)

    teamA_id: number;
    teamB_id: number;
    teamA_name: string;
    teamB_name: string;
    teamA_score: number; // 当前血量
    teamB_score: number; // 当前血量

    teamA_player_order_ids: number[];
    teamB_player_order_ids: number[];
    teamA_current_player_id: number | null; // 当前代表A队出战的选手ID
    teamB_current_player_id: number | null; // 当前代表B队出战的选手ID
    // Denormalized current player info (fetched from teamA_members/teamB_members)
    teamA_current_player_nickname?: string;
    teamB_current_player_nickname?: string;
    teamA_current_player_profession?: string | null; // '绝剑士', '矩盾手', '炼星师'

    teamA_mirror_available: boolean;
    teamB_mirror_available: boolean;

    match_song_list: MatchSong[]; // 整场比赛的歌单 (DO 内部也需要完整歌单)
    current_song: MatchSong | null; // 当前正在进行的歌曲 (match_song_list 中的一项)

    roundSummary: RoundSummary | null; // 上一轮/刚结束的这一轮的计算总结
}

// Payload for initializing DO from D1 TournamentMatch data
export interface MatchScheduleData {
    tournamentMatchId: number;
    round_name: string;
    team1_id: number;
    team2_id: number;
    team1_name: string; // Denormalized
    team2_name: string; // Denormalized
    team1_members: Member[]; // Full member objects for Team A
    team2_members: Member[]; // Full member objects for Team B
    team1_player_order_ids: number[]; // Ordered member IDs
    team2_player_order_ids: number[]; // Ordered member IDs
    match_song_list: MatchSong[]; // The pre-configured song list for this match
}


// 提交成绩的 Payload (前端发送完整百分比)
export interface CalculateRoundPayload {
    teamA_percentage: number; // e.g., 100.1234
    teamB_percentage: number; // e.g., 98.7654
    teamA_effect_value?: number; // 小分调整
    teamB_effect_value?: number; // 小分调整
}

// Payload for resolving a draw (Staff action)
export interface ResolveDrawPayload {
    winner: 'teamA' | 'teamB';
}

// Payload for Staff selecting a tiebreaker song
export interface SelectTiebreakerSongPayload {
    song_id: number;
    selected_difficulty: string; // e.g., 'M', 'E' (Difficulty level key from SongLevel)
    // Worker/DO will need to fetch song details and construct the MatchSong object
}

// 回合总结 (用于展示计算过程和历史记录) - 对应 match_rounds_history 表的部分字段 + 详细计算日志
export interface RoundSummary {
    round_number_in_match: number; // 这首歌是比赛的第 N 首 (1-based)
    song_id: number;
    song_title: string;
    selected_difficulty: string; // 实际打的难度，例如 'M 13'

    teamA_player_id: number;
    teamB_player_id: number;
    teamA_player_nickname: string;
    teamB_player_nickname: string;

    teamA_percentage: number;
    teamB_percentage: number;
    teamA_effect_value_applied: number; // 小分调整
    teamB_effect_value_applied: number;

    // 详细计算步骤 (来自 DO 内部逻辑)
    teamA_damage_digits: number[];
    teamB_damage_digits: number[];
    teamA_base_damage: number;
    teamB_base_damage: number;
    teamA_profession?: string | null; // Player profession
    teamB_profession?: string | null;
    teamA_profession_effect_applied?: string; // e.g., "Attacker: +5 damage"
    teamB_profession_effect_applied?: string;
    teamA_modified_damage_to_B: number; // Damage A deals to B after A's profession effect
    teamB_modified_damage_to_A: number; // Damage B deals to A after B's profession effect

    teamA_health_before_round: number; // Health at start of this round
    teamB_health_before_round: number;

    teamA_mirror_triggered: boolean;
    teamB_mirror_triggered: boolean;
    teamA_mirror_effect_applied?: string; // e.g., "Defender: Reflected 10 damage"
    teamB_mirror_effect_applied?: string;
    teamA_supporter_base_skill_heal?: number; // Heal from supporter base skill
    teamB_supporter_base_skill_heal?: number;
    teamA_supporter_mirror_bonus_heal?: number; // Heal from supporter mirror bonus
    teamB_supporter_mirror_bonus_heal?: number;

    teamA_final_damage_dealt: number; // Total damage A caused to B (incl. attacker/defender mirror)
    teamB_final_damage_dealt: number; // Total damage B caused to A (incl. attacker/defender mirror)

    teamA_health_change: number; // Total health change for A this round
    teamB_health_change: number; // Total health change for B this round
    teamA_health_after: number; // Health after this round
    teamB_health_after: number;

    is_tiebreaker_song?: boolean; // Whether this round was a tiebreaker

    log?: string[]; // Optional detailed calculation log
}

// API 响应包装
export interface ApiResponse<T = any> {
    success: boolean;
    data?: T;
    error?: string;
    message?: string;
}

// Internal profession type used in DO logic
export type InternalProfession = 'attacker' | 'defender' | 'supporter' | null;
