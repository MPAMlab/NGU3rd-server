// Represents the real-time state stored in the Durable Object
export interface MatchState {
    matchId: string; // The ID of the Durable Object instance
    round: number;
    teamA_name: string;
    teamA_score: number;
    teamA_player: string;
    teamB_name: string;
    teamB_score: number;
    teamB_player: string;
    status: 'pending' | 'live' | 'finished' | 'paused' | 'archived_in_d1'; // 'archived_in_d1' means the whole match DO instance is marked as archived
  }
  
  // Type for a single archived round record stored in D1
  export interface RoundArchive {
      id: number; // D1 auto-increment ID
      match_do_id: string; // The DO ID this round belongs to
      round_number: number;
      team_a_name: string;
      team_a_score: number;
      team_a_player: string;
      team_b_name: string;
      team_b_score: number;
      team_b_player: string;
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
    match_name?: string; // Optional: Name for the overall match/tournament stage
    final_round?: number; // The round number when the match was archived
    team_a_name?: string; // Final names
    team_a_score?: number; // Final scores
    team_a_player?: string; // Final players
    team_b_name?: string;
    team_b_score?: number;
    team_b_player?: string;
    status: string; // Final status (should be 'finished' or 'archived_in_d1' in D1)
    archived_at: string; // ISO date string
    raw_data?: string; // Optional: store raw JSON of the final state
    winner_team_name?: string; // Added winner field for the whole match
  }
  
  export interface Env {
    MATCH_DO: DurableObjectNamespace;
    DB: D1Database; // Already in your wrangler.jsonc
    AVATAR_BUCKET: R2Bucket; // Already in your wrangler.jsonc
    // Add any other bindings or environment variables here
  }
  