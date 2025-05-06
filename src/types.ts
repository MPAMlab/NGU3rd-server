export interface MatchState {
    matchId: string;
    round: number;
    teamA_name: string;
    teamA_score: number;
    teamA_player: string;
    teamB_name: string;
    teamB_score: number;
    teamB_player: string;
    status: 'pending' | 'live' | 'finished' | 'paused' | 'archived_in_d1'; // Added 'archived_in_d1'
  }
  
  export const initialMatchStateValues: Omit<MatchState, 'matchId'> = {
    round: 1,
    teamA_name: '队伍A',
    teamA_score: 0,
    teamA_player: '选手A1',
    teamB_name: '队伍B',
    teamB_score: 0,
    teamB_player: '选手B1',
    status: 'pending',
  };
  
  export interface Env {
    MATCH_DO: DurableObjectNamespace;
    DB: D1Database; // Already in your wrangler.jsonc
    AVATAR_BUCKET: R2Bucket; // Already in your wrangler.jsonc
    // Add any other bindings or environment variables here
  }
  