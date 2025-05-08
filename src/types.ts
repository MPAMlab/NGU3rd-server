// src/types.ts

// Represents the environment variables and bindings
export interface Env {
    MATCH_DO: DurableObjectNamespace;
    DB: D1Database;
    AVATAR_BUCKET: R2Bucket;
    // Add any other bindings or environment variables here
  }

  // Represents a Team (matching your existing table structure)
  export interface Team {
      id: number; // D1 auto-increment ID
      code: string; // 4-character manual ID (your team_code)
      name: string; // your team_name
      created_at?: number | null; // Unix timestamp (matching your table)
      current_health?: number | null;
      has_revive_mirror?: number | null; // 0 or 1
      status?: string | null; // e.g., 'active', 'inactive'
  }

  // Represents a Member (matching your existing table structure)
  export interface Member {
      id: number; // D1 auto-increment ID
      team_code: string; // FK to teams.code
      color?: string | null;
      job?: string | null; // e.g., "绝剑士", "矩盾手", "炼星师"
      maimai_id?: string | null;
      nickname: string; // The player name to display
      qq_number?: string | null;
      avatar_url?: string | null;
      joined_at?: number | null; // Unix timestamp
      updated_at?: number | null; // Unix timestamp
      kinde_user_id?: string | null;
      is_admin?: number | null; // 0 or 1
      // Note: 'name' field is not in DB schema but used in DO state.
      // Consider adding 'name: string;' here if you want Member type to always have it,
      // or map nickname to name when creating DO state.
      // For now, assuming nickname is used as player name.
  }


  // Represents a scheduled Tournament Match (linking to your teams.id)
  export interface TournamentMatch {
      id: number;
      tournament_round: string; // e.g., '八进四', '四进二', '决赛'
      match_number_in_round: number;
      team1_id: number; // FK to teams.id
      team2_id: number; // FK to teams.id
      team1_player_order?: string | null; // e.g., '1,2,3' (string)
      team2_player_order?: string | null; // e.g., '1,2,3' (string)
      match_do_id?: string | null; // NULLABLE, Associated DO ID when live
      status: 'scheduled' | 'live' | 'completed' | 'archived'; // Status of the scheduled match entry
      winner_team_id?: number | null; // NULLABLE, FK to teams.id (比赛结束后填写)
      scheduled_time?: string | null; // Optional: ISO 8601 string preferred
      created_at: string; // ISO 8601 timestamp

      // Fields added by JOIN in the query (used in worker responses)
      team1_code?: string;
      team1_name?: string;
      team2_code?: string;
      team2_name?: string;
      winner_team_code?: string;
      winner_team_name?: string;
  }

  // --- New Type for Tournament Match Form Data / Create Payload ---
  // This type allows team_id to be null for form state before submission
  export interface CreateTournamentMatchPayload {
      tournament_round: string;
      match_number_in_round: number;
      team1_id: number | null; // Allow null in form state
      team2_id: number | null; // Allow null in form state
      team1_player_order?: string | null;
      team2_player_order?: string | null;
      scheduled_time?: string | null;
      // Note: status, match_do_id, winner_team_id, created_at are not part of the creation payload
  }


  // Represents the real-time state stored in the Durable Object
  // This type is primarily used within the DO and when passing data to initialize it
  export interface MatchState {
    matchId: string; // The ID of the Durable Object instance (should match tournamentMatchId.toString())
    tournamentMatchId?: number | null; // Added: Link back to the scheduled match

    round: number;
    teamA_name: string;
    teamA_score: number;
    teamA_player: string; // Current player nickname
    teamB_name: string;
    teamB_score: number;
    teamB_player: string; // Current player nickname

    // Store full member objects and the order of their IDs
    teamA_members: Member[]; // Array of Member objects for Team A
    teamB_members: Member[]; // Array of Member objects for Team B
    teamA_player_order_ids: number[]; // Array of member.id in the desired playing order
    teamB_player_order_ids: number[]; // Array of member.id in the desired playing order
    current_player_index_a: number; // Index in teamA_player_order_ids for current player
    current_player_index_b: number; // Index in teamB_player_order_ids for current player

    // New fields for game logic
    teamA_mirror_available: boolean;
    teamB_mirror_available: boolean;
    teamA_current_player_profession: 'attacker' | 'defender' | 'supporter' | null;
    teamB_current_player_profession: 'attacker' | 'defender' | 'supporter' | null;


    status: 'pending' | 'round_finished' | 'team_A_wins' | 'team_B_wins' | 'draw_pending_resolution' | 'archived_in_d1';
  }

  // Type for a single archived round record stored in D1
  export interface RoundArchive {
      id: number; // D1 auto-increment ID
      match_do_id: string; // The DO ID this round belongs to
      round_number: number;
      team_a_name: string;
      team_a_score: number;
      team_a_player: string; // Player nickname at the end of the round
      team_b_name: string;
      team_b_score: number;
      team_b_player: string; // Player nickname at the end of the round
      status: string; // Status of the match when this round was archived (e.g., 'finished' for the round)
      archived_at: string; // ISO date string
      raw_data?: string; // Optional: store raw JSON of the state at that moment
      winner_team_name?: string; // Added winner field
      // is_editable?: number; // If you add this field
  }


  // Type for a single archived match summary record stored in D1
  export interface MatchArchiveSummary {
    id: number; // D1 auto-increment ID
    match_do_id: string; // The DO ID this match belongs to (unique)
    tournament_match_id?: number | null; // Added: Link back to the scheduled match
    match_name?: string; // Optional: Name for the overall match/tournament stage
    final_round?: number; // The round number when the match was archived
    team_a_name?: string; // Final names
    team_a_score?: number; // Final scores
    team_a_player?: string; // Final players
    team_b_name?: string;
    team_b_score?: number;
    team_b_player?: string;
    status: string; // Final status (should be 'completed' or 'archived_in_d1' in D1)
    archived_at: string; // ISO date string
    raw_data?: string; // Optional: store raw JSON of the final state
    winner_team_name?: string; // Added winner field for the whole match
  }

  // --- Types for Bulk Import ---

  // Expected structure for a row in the Team Bulk Import CSV
  export interface BulkTeamRow {
      code: string;
      name: string;
      // Add other optional fields if you want to import them initially
      // current_health?: string; // CSV is string, need parsing
      // has_revive_mirror?: string;
      // status?: string;
  }

  // Expected structure for a row in the Member Bulk Import CSV
  export interface BulkMemberRow {
      team_code: string;
      nickname: string;
      color?: string;
      job?: string;
      maimai_id?: string;
      qq_number?: string;
      avatar_url?: string;
      kinde_user_id?: string;
      is_admin?: string; // CSV is string, need parsing to number/boolean
  }

  // Expected structure for a row in the Tournament Match Bulk Import CSV
  // Similar to CreateTournamentMatchPayload, but values come as strings from CSV parsing
  export interface BulkTournamentMatchRow {
      tournament_round: string;
      match_number_in_round: string; // Comes as string from CSV
      team1_id: string; // Comes as string from CSV
      team2_id: string; // Comes as string from CSV
      team1_player_order?: string; // Comes as string from CSV
      team2_player_order?: string; // Comes as string from CSV
      scheduled_time?: string; // Comes as string from CSV
  }

  // --- Type for data passed to DO initialization from schedule ---
  // This is the structure sent in the body of the POST to /internal/initialize-from-schedule
  export interface MatchScheduleData {
      tournamentMatchId: number;
      team1_name: string;
      team2_name: string;
      team1_members: Member[]; // Full member objects
      team2_members: Member[]; // Full member objects
      team1_player_order_ids: number[]; // Ordered member IDs
      team2_player_order_ids: number[]; // Ordered member IDs
      // You might also want to pass round/match number for initial state display
      round_name?: string; // Added
      match_number_in_round?: number; // Added
  }

  // Payload for calculating a round
  export interface CalculateRoundPayload {
      teamA_percentage: number;
      teamB_percentage: number;
  }

  // Payload for resolving a draw
  export interface ResolveDrawPayload {
      winner: 'teamA' | 'teamB';
  }

  // Internal profession type used in DO logic
  export type InternalProfession = 'attacker' | 'defender' | 'supporter' | null;
