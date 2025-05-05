// src/types.ts

// Standard Worker types (already imported in index.ts, but good to list here for clarity)
// import { D1Database, R2Bucket, ExecutionContext } from "@cloudflare/workers-types";

// Define environment variables/bindings
export interface Env {
    DB: D1Database;
    R2_AVATARS: R2Bucket;
    // Kinde App 2 Secrets (matching App 1 config names)
    KINDE_CLIENT_ID: string;
    KINDE_CLIENT_SECRET: string;
    KINDE_ISSUER_URL: string; // e.g., https://ngu3rd.kinde.com
    KINDE_REDIRECT_URI: string; // e.g., https://ngu3rd.mpam-lab.xyz/callback
    LOGOUT_REDIRECT_TARGET_URL: string; // e.g., https://ngu3rd.mpam-lab.xyz/
    // Durable Object Binding
    LIVE_MATCH_DO: DurableObjectNamespace;
    // Other vars
    R2_PUBLIC_URL_BASE: string; // e.g., https://pub-your-r2-id.r2.dev
    // Add other secrets/vars as needed
  }
  
  // Kinde User Info (from token payload)
  export interface KindeUser {
    id: string; // kinde_user_id (sub claim)
    email: string;
    given_name?: string;
    family_name?: string;
    // Add other claims you need
  }
  
  // Your Member structure (from D1) - Ensure is_admin is included
  export interface Member {
    id: number;
    kinde_user_id: string; // Link to Kinde
    maimai_id: string;
    name: string; // This might be nickname or real name depending on your DB
    nickname: string; // Added nickname based on App 1 code
    qq_number: string; // Added qq_number based on App 1 code
    team_code: string | null;
    element: 'red' | 'green' | 'blue'; // Use lowercase matching App 1 code
    profession: 'attacker' | 'defender' | 'supporter'; // Use lowercase matching App 1 code
    is_admin: 0 | 1; // 0 for user, 1 for admin
    avatar_url: string | null;
    joined_at: number; // Timestamp
    updated_at: number; // Timestamp
  }
  
  // Your Team structure (from D1)
  export interface Team {
    id: number;
    code: string;
    name: string;
    current_health: number;
    has_revive_mirror: 0 | 1; // 1 for true, 0 for false
    status: string; // 'active', 'eliminated_prelim', etc.
    created_at: number;
    updated_at: number;
  }
  
  // Your Match structure (from D1)
  export interface Match {
    id: number;
    stage: 'prelim' | 'semi' | 'final'; // '初赛', '半决赛', '决赛' - use consistent string keys
    round_number: number; // e.g., 8进4, 4进2, 2进1
    team1_code: string;
    team2_code: string;
    status: 'scheduled' | 'active' | 'completed'; // '未开始', '进行中', '已结束'
    winner_team_code: string | null;
    current_song_index: number; // 0-indexed, represents the turn number
    created_at: number;
    updated_at: number;
  }
  
  // Your MatchTurn structure (from D1)
  export interface MatchTurn {
      id: number;
      match_id: number;
      song_index: number; // 0-indexed turn number
      song_id: number | null; // Link to songs table if applicable
      song_name_override: string; // Song name played
      difficulty_level_played: string; // e.g., 'EXP', 'MST', 'Re:M'
      playing_member_id_team1: number; // Member ID from team 1
      playing_member_id_team2: number; // Member ID from team 2
      score_percent_team1: string; // e.g., "100.4902"
      score_percent_team2: string;
      calculated_damage_team1: number; // Damage dealt by team 1 (before negation)
      calculated_damage_team2: number; // Damage dealt by team 2 (before negation)
      health_change_team1: number; // Net health change for team 1
      health_change_team2: number; // Net health change for team 2
      team1_health_before: number;
      team2_health_before: number;
      team1_health_after: number;
      team2_health_after: number;
      team1_revive_used_this_turn: 0 | 1;
      team2_revive_used_this_turn: 0 | 1;
      recorded_by_staff_id: number; // Staff member ID who recorded this turn
      recorded_at: number; // Timestamp
      calculation_log: string; // JSON string of the calculation log
  }
  
  // Your Song structure (from D1 or JSON)
  export interface Song {
      id: number; // If from DB
      name: string;
      artist: string;
      image_url: string;
      difficulties: { // Structure might vary, example
          BAS?: number;
          ADV?: number;
          EXP?: number;
          MST?: number;
          ReM?: number;
      };
      // Add other song properties
  }
  
  // Type for the real-time match state broadcasted via WebSocket
  export interface LiveMatchState {
    matchId: number;
    stage: Match['stage'];
    roundNumber: Match['round_number'];
    team1Code: string;
    team2Code: string;
    team1Name: string;
    team2Name: string;
    team1Health: number;
    team2Health: number;
    team1HasMirror: boolean;
    team2HasMirror: boolean;
    currentSongIndex: number; // 0-indexed turn number
    status: Match['status'];
    winnerTeamCode: string | null;
    // Add other data needed for the live page
    lastTurnLog?: string[]; // Optional: Include log for the last completed turn
    currentTurnInfo?: { // Info about the turn that *just* finished
        songName: string;
        difficulty: string;
        team1PlayerName: string;
        team2PlayerName: string;
        team1PlayerMaimaiId: string;
        team2PlayerMaimaiId: string;
        // Add other relevant info
    };
    // Maybe include info about the *next* turn's players/song if pre-determined
    nextTurnInfo?: {
        songName: string;
        difficulty: string;
        team1PlayerName: string;
        team2PlayerName: string;
        team1PlayerMaimaiId: string;
        team2PlayerMaimaiId: string;
    } | null;
  }
  
  
  // Input for damage calculation function (already defined, just re-listing)
  export interface DamageCalculationInput {
    team1Health: number;
    team2Health: number;
    team1HasMirror: boolean;
    team2HasMirror: boolean;
    scorePercent1: string; // e.g., "100.4902"
    scorePercent2: string; // e.g., "100.9105"
    team1Profession: Member['profession'];
    team2Profession: Member['profession'];
  }
  
  // Output of damage calculation function (already defined, just re-listing)
  export interface DamageCalculationResult {
    team1HealthAfter: number;
    team2HealthAfter: number;
    team1MirrorUsedThisTurn: boolean;
    team2MirrorUsedThisTurn: boolean;
    team1DamageDealt: number; // Damage calculated from score + skills before negation
    team2DamageDealt: number; // Damage calculated from score + skills before negation
    team1DamageTaken: number; // Damage taken after opponent's negation
    team2DamageTaken: number; // Damage taken after opponent's negation
    team1HealthChange: number; // Net change in health
    team2HealthChange: number; // Net change in health
    log: string[]; // Step-by-step log of the calculation
  }
  
  // Input for the record turn API handler (already defined, just re-listing)
  export interface RecordTurnInput {
    matchId: number;
    team1MemberId: number; // The member who played for team 1
    team2MemberId: number; // The member who played for team 2
    scorePercent1: string;
    scorePercent2: string;
    difficultyLevelPlayed: string; // e.g., 'M', 'R'
    songName: string; // The name of the song played
    songId?: number; // Optional, if it's a staff-added song from the 'songs' table
  }
  
  // Authenticated Request with Kinde User and Member Info
  // This is what handlers will receive after authMiddleware
  export interface AuthenticatedRequest extends Request {
    kindeUser?: KindeUser; // Basic info from Kinde token
    member?: Member; // Your member data from D1
    isAdmin?: boolean; // Flag from member data
    params?: { [key: string]: string }; // For manual path parameter extraction
  }
  