// src/durable-objects/matchDo.ts
import type {
  MatchState,
  Env,
  Member,
  MatchScheduleData,
  CalculateRoundPayload,
  ResolveDrawPayload,
  InternalProfession,
  MatchSong, // Import MatchSong
  RoundSummary, // Import RoundSummary
  SelectTiebreakerSongPayload, // Import SelectTiebreakerSongPayload
  Song // Import Song to fetch details for tiebreakers
} from '../types';

// Constants for game logic
const INITIAL_HEALTH = 100;
const MIRROR_HEALTH_RESTORE = 20;
const MAX_DAMAGE_DIGIT = 10; // 0% completion corresponds to 10 damage
const STANDARD_ROUNDS_COUNT = 6; // Number of standard rounds (BO6)

// Default state for a new match (fallback, initialization should come from schedule)
// This default state is less relevant now as initialization comes from D1
const defaultMatchState: Omit<MatchState, 'match_do_id' | 'tournament_match_id' | 'round_name' | 'teamA_id' | 'teamB_id' | 'teamA_name' | 'teamB_name' | 'teamA_player_order_ids' | 'teamB_player_order_ids' | 'match_song_list'> = {
  status: 'archived', // Default to archived if not initialized properly
  current_match_song_index: 0,
  teamA_score: INITIAL_HEALTH,
  teamB_score: INITIAL_HEALTH,
  teamA_current_player_id: null,
  teamB_current_player_id: null,
  teamA_current_player_nickname: '未知选手',
  teamB_current_player_nickname: '未知选手',
  teamA_current_player_profession: null,
  teamB_current_player_profession: null,
  teamA_mirror_available: true,
  teamB_mirror_available: true,
  current_song: null,
  roundSummary: null,
};


export class MatchDO implements DurableObject {
  state: DurableObjectState;
  env: Env;
  matchData: MatchState | null = null;
  match_do_id: string; // Renamed from matchId to match_do_id for clarity
  websockets: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
      this.state = state;
      this.env = env;
      this.match_do_id = state.id.toString();

      // Load state from storage on DO startup
      this.state.blockConcurrencyWhile(async () => {
          const storedMatchData = await this.state.storage.get<MatchState>('matchData');
          if (storedMatchData) {
              this.matchData = storedMatchData;
              // Basic migration/initialization for new fields if loading old state
              // This is crucial if you deploy new DO code with added fields
              if (this.matchData && this.matchData.match_song_list === undefined) {
                  console.warn(`DO (${this.match_do_id}): Initializing new fields for old state.`);
                  // Initialize new fields with default/safe values
                  this.matchData.match_song_list = this.matchData.match_song_list ?? [];
                  this.matchData.current_song = this.matchData.current_song ?? null;
                  this.matchData.roundSummary = this.matchData.roundSummary ?? null;
                  this.matchData.round_name = (this.matchData as any).tournament_round ?? '未知轮次'; // Map old field if exists
                  this.matchData.teamA_id = (this.matchData as any).teamA_id ?? -1; // Map old field if exists
                  this.matchData.teamB_id = (this.matchData as any).teamB_id ?? -1; // Map old field if exists
                  this.matchData.teamA_current_player_id = this.matchData.teamA_current_player_id ?? null;
                  this.matchData.teamB_current_player_id = this.matchData.teamB_current_player_id ?? null;
                  this.matchData.teamA_current_player_nickname = this.matchData.teamA_current_player_nickname ?? '未知选手';
                  this.matchData.teamB_current_player_nickname = this.matchData.teamB_current_player_nickname ?? '未知选手';
                  this.matchData.teamA_current_player_profession = this.matchData.teamA_current_player_profession ?? null;
                  this.matchData.teamB_current_player_profession = this.matchData.teamB_current_player_profession ?? null;

                  // Attempt to map old status values if necessary
                  if ((this.matchData as any).status === 'pending') this.matchData.status = 'pending_scores';
                  if ((this.matchData as any).status === 'round_finished') this.matchData.status = 'round_finished';
                  if ((this.matchData as any).status === 'archived_in_d1') this.matchData.status = 'archived';
                  // Add other status mappings as needed

                  await this.state.storage.put('matchData', this.matchData); // Persist updated structure
              }
               // Ensure current_song is correctly set based on index and list on load
               if (this.matchData && this.matchData.match_song_list && this.matchData.match_song_list.length > this.matchData.current_match_song_index) {
                   this.matchData.current_song = this.matchData.match_song_list[this.matchData.current_match_song_index];
               } else {
                   this.matchData.current_song = null;
               }
               // Ensure current players are set based on index and order on load
               if (this.matchData && this.matchData.teamA_player_order_ids && this.matchData.teamA_player_order_ids.length > 0) {
                    const playerAId = this.matchData.teamA_player_order_ids[this.matchData.current_match_song_index % this.matchData.teamA_player_order_ids.length];
                    const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
                    this.matchData.teamA_current_player_id = playerAId;
                    this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
                    this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
               } else {
                    this.matchData.teamA_current_player_id = null;
                    this.matchData.teamA_current_player_nickname = '未知选手';
                    this.matchData.teamA_current_player_profession = null;
               }
               if (this.matchData && this.matchData.teamB_player_order_ids && this.matchData.teamB_player_order_ids.length > 0) {
                    const playerBId = this.matchData.teamB_player_order_ids[this.matchData.current_match_song_index % this.matchData.teamB_player_order_ids.length];
                    const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);
                    this.matchData.teamB_current_player_id = playerBId;
                    this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
                    this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);
               } else {
                    this.matchData.teamB_current_player_id = null;
                    this.matchData.teamB_current_player_nickname = '未知选手';
                    this.matchData.teamB_current_player_profession = null;
               }


          } else {
              // DO is being created for the first time for this ID.
              // It MUST be initialized via /internal/initialize-from-schedule.
              // Initialize with a minimal state indicating it's not ready.
              console.warn(`DO (${this.match_do_id}): Initializing with minimal state. Waiting for schedule initialization.`);
              this.matchData = {
                  match_do_id: this.match_do_id,
                  tournament_match_id: -1, // Indicate not linked yet
                  status: 'scheduled', // Or 'uninitialized'
                  round_name: '未知轮次',
                  current_match_song_index: 0,
                  teamA_id: -1, teamB_id: -1,
                  teamA_name: '未知队伍A', teamB_name: '未知队伍B',
                  teamA_score: INITIAL_HEALTH, teamB_score: INITIAL_HEALTH,
                  teamA_player_order_ids: [], teamB_player_order_ids: [],
                  teamA_current_player_id: null, teamB_current_player_id: null,
                  teamA_current_player_nickname: '未知选手', teamB_current_player_nickname: '未知选手',
                  teamA_current_player_profession: null, teamB_current_player_profession: null,
                  teamA_mirror_available: true, teamB_mirror_available: true,
                  match_song_list: [],
                  current_song: null,
                  roundSummary: null,
              };
              // No need to await put here, initializeFromSchedule will save it.
          }
      });
  }

  // Helper to map job string to internal profession enum
  private getInternalProfession(job?: string | null): InternalProfession {
      if (!job) return null;
      const lowerJob = job.toLowerCase();
      if (lowerJob.includes('绝剑士') || lowerJob.includes('attacker')) return 'attacker';
      if (lowerJob.includes('矩盾手') || lowerJob.includes('defender')) return 'defender';
      if (lowerJob.includes('炼星师') || lowerJob.includes('supporter')) return 'supporter';
      console.warn(`DO (${this.match_do_id}): Unknown profession string "${job}". Returning null.`);
      return null;
  }

  // Helper to get a member by ID from a list
  private getMemberById(memberId: number | undefined | null, members: Member[] | undefined): Member | undefined {
      if (memberId === undefined || memberId === null || members === undefined) {
          return undefined;
      }
      return members.find(m => m.id === memberId);
  }

  // Helper to get player nickname based on member ID and members list
  private getPlayerNickname(memberId: number | undefined | null, members: Member[] | undefined): string {
      const member = this.getMemberById(memberId, members);
      return member?.nickname || '未知选手';
  }

  // Helper to parse damage digits from percentage (0-101.0000)
  // This now expects the full percentage value (e.g., 100.1234)
  private parseDamageDigits(percentage: number): number[] {
      // Ensure percentage is within a reasonable range (e.g., 0 to 101.0000 for maimai)
      const clampedPercentage = Math.max(0, Math.min(101.0000, percentage));

      // Convert to string with exactly 4 decimal places
      const percentageString = clampedPercentage.toFixed(4);
      const parts = percentageString.split('.');

      if (parts.length !== 2) {
          console.error(`DO (${this.match_do_id}): Unexpected percentage format after toFixed: ${percentageString} (original: ${percentage})`);
          return [MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT]; // Return max damage on error or all 10s
      }

      const digitsString = parts[1]; // Get the decimal part as string (e.g., "2345")
      const digits: number[] = [];
      for (let i = 0; i < 4; i++) {
          const digitChar = digitsString[i] || '0';
          const digit = parseInt(digitChar, 10);
          // Map 0 to 10 for damage calculation
          digits.push(digit === 0 ? MAX_DAMAGE_DIGIT : digit);
      }
      // console.log(`parseDamageDigits: input=${percentage}, clamped=${clampedPercentage}, string=${percentageString}, digitsString=${digitsString}, outputDigits=${digits}`);
      return digits;
  }


  private broadcast(message: object | string) {
      const payload = typeof message === 'string' ? message : JSON.stringify(message);
      this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
      this.websockets.forEach((ws) => {
          try {
              ws.send(payload);
          } catch (e) {
              console.error('Error sending message to WebSocket:', e);
          }
      });
  }

  private determineWinnerTeamId(state: { teamA_score: number; teamB_score: number; teamA_id: number; teamB_id: number }): number | null {
      if (state.teamA_score > state.teamB_score) {
          return state.teamA_id;
      } else if (state.teamB_score > state.teamA_score) {
          return state.teamB_id;
      } else {
          return null; // Draw or undecided
      }
  }

  // Helper to get the current players based on song index and player order
  private getCurrentPlayers(state: MatchState): { playerAId: number | null, playerBId: number | null } {
      const playerAId = state.teamA_player_order_ids && state.teamA_player_order_ids.length > 0
          ? state.teamA_player_order_ids[state.current_match_song_index % state.teamA_player_order_ids.length]
          : null;
      const playerBId = state.teamB_player_order_ids && state.teamB_player_order_ids.length > 0
          ? state.teamB_player_order_ids[state.current_match_song_index % state.teamB_player_order_ids.length]
          : null;
      return { playerAId, playerBId };
  }


  // --- Internal Method: Initialize from Schedule ---
  // Called by the Worker when starting a match from the schedule
  private async initializeFromSchedule(scheduleData: MatchScheduleData): Promise<{ success: boolean; message?: string }> {
      console.log(`DO (${this.match_do_id}): Initializing from schedule for tournament match ${scheduleData.tournamentMatchId}`);

      // Clear existing state if any, only if it's not already a live match for this ID
      // and the existing state is not already linked to this tournamentMatchId
      if (this.matchData?.tournament_match_id !== scheduleData.tournamentMatchId || this.matchData?.status === 'archived') {
          await this.state.storage.deleteAll();
          console.log(`DO (${this.match_do_id}): Cleared storage for new initialization.`);
      } else {
          console.log(`DO (${this.match_do_id}): Already initialized for tournament match ${scheduleData.tournamentMatchId}. Skipping storage clear.`);
          // If already initialized for this match and not archived, maybe just return success?
          // Or re-initialize if the status is not 'pending_scores'?
          if (this.matchData?.status !== 'pending_scores') {
               console.warn(`DO (${this.match_do_id}): Match is in status ${this.matchData.status}. Re-initializing.`);
          } else {
               console.log(`DO (${this.match_do_id}): Match is already pending scores. Returning current state.`);
               this.broadcast(this.matchData); // Broadcast current state
               return { success: true, message: "Match already initialized and pending scores." };
          }
      }


      // Validate player orders and members
      const validateOrder = (orderIds: number[], members: Member[], teamName: string) => {
          if (!Array.isArray(orderIds) || orderIds.length === 0) {
              console.error(`DO (${this.match_do_id}): Invalid or empty player order for ${teamName}.`);
              return false;
          }
          for (const id of orderIds) {
              if (!members.find(m => m.id === id)) {
                  console.error(`DO (${this.match_do_id}): Player ID ${id} in order for ${teamName} not found in member list.`);
                  return false;
              }
          }
          return true;
      };

      if (!validateOrder(scheduleData.team1_player_order_ids, scheduleData.team1_members, scheduleData.team1_name) ||
          !validateOrder(scheduleData.team2_player_order_ids, scheduleData.team2_members, scheduleData.team2_name)) {
          const msg = "Invalid player order or member data provided for initialization.";
          console.error(`DO (${this.match_do_id}): ${msg}`);
          return { success: false, message: msg };
      }

      // Validate song list
      if (!Array.isArray(scheduleData.match_song_list) || scheduleData.match_song_list.length === 0) {
           const msg = "Invalid or empty match song list provided for initialization.";
           console.error(`DO (${this.match_do_id}): ${msg}`);
           return { success: false, message: msg };
      }
      // TODO: Add validation for individual MatchSong objects (e.g., song_id, picker_member_id exist)


      // Determine initial players
      const { playerAId, playerBId } = this.getCurrentPlayers({
           ...defaultMatchState, // Use default for structure, override with scheduleData
           match_do_id: this.match_do_id,
           tournament_match_id: scheduleData.tournamentMatchId,
           teamA_player_order_ids: scheduleData.team1_player_order_ids,
           teamB_player_order_ids: scheduleData.team2_player_order_ids,
           current_match_song_index: 0, // Start at index 0
           // Need other fields for getCurrentPlayers helper, but only player orders and index are used
           teamA_id: scheduleData.team1_id, teamB_id: scheduleData.team2_id,
           teamA_name: scheduleData.team1_name, teamB_name: scheduleData.team2_name,
           teamA_score: INITIAL_HEALTH, teamB_score: INITIAL_HEALTH,
           teamA_members: scheduleData.team1_members, teamB_members: scheduleData.team2_members,
           teamA_mirror_available: true, teamB_mirror_available: true,
           match_song_list: scheduleData.match_song_list,
           current_song: null, roundSummary: null, round_name: scheduleData.round_name,
           teamA_current_player_id: null, teamB_current_player_id: null,
           teamA_current_player_nickname: '未知选手', teamB_current_player_nickname: '未知选手',
           teamA_current_player_profession: null, teamB_current_player_profession: null,
      });

      const memberA = this.getMemberById(playerAId, scheduleData.team1_members);
      const memberB = this.getMemberById(playerBId, scheduleData.team2_members);


      // Initialize matchData from schedule data
      this.matchData = {
          match_do_id: this.match_do_id,
          tournament_match_id: scheduleData.tournamentMatchId,
          round_name: scheduleData.round_name,
          current_match_song_index: 0, // Always start at index 0 for a new live match
          teamA_id: scheduleData.team1_id,
          teamB_id: scheduleData.team2_id,
          teamA_name: scheduleData.team1_name,
          teamB_name: scheduleData.team2_name,
          teamA_score: INITIAL_HEALTH, // Start with full health
          teamB_score: INITIAL_HEALTH, // Start with full health
          teamA_members: scheduleData.team1_members, // Store full member objects
          teamB_members: scheduleData.team2_members,
          teamA_player_order_ids: scheduleData.team1_player_order_ids, // Store ordered IDs
          teamB_player_order_ids: scheduleData.team2_player_order_ids,
          teamA_current_player_id: playerAId, // Set initial players
          teamB_current_player_id: playerBId,
          teamA_current_player_nickname: memberA?.nickname || '未知选手',
          teamB_current_player_nickname: memberB?.nickname || '未知选手',
          teamA_current_player_profession: this.getInternalProfession(memberA?.job),
          teamB_current_player_profession: this.getInternalProfession(memberB?.job),
          teamA_mirror_available: true, // Mirror available at start
          teamB_mirror_available: true, // Mirror available at start
          match_song_list: scheduleData.match_song_list, // Store the full song list
          current_song: scheduleData.match_song_list[0] || null, // Set initial current song
          roundSummary: null, // No summary yet
          status: 'pending_scores', // Start as pending, waiting for scores for the first song
      };

      // Mark the first song as ongoing
      if (this.matchData.current_song) {
           this.matchData.current_song.status = 'ongoing';
      }


      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);
          console.log(`DO (${this.match_do_id}): State initialized from schedule.`);
          return { success: true, message: "Match initialized from schedule." };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}): Failed to save initial state from schedule:`, e);
          return { success: false, message: `Failed to initialize match: ${e.message}` };
      }
  }

  // --- Core Game Logic: Calculate Round Outcome ---
  private async calculateRoundOutcome(payload: CalculateRoundPayload): Promise<{ success: boolean; message?: string; roundSummary?: RoundSummary }> {
      if (!this.matchData) {
          const msg = "Match data not initialized.";
          console.error(`DO (${this.match_do_id}): ${msg}`);
          return { success: false, message: msg };
      }
      // Check if status allows calculation
      if (this.matchData.status !== 'pending_scores') {
          const msg = `Match is not in 'pending_scores' status (${this.matchData.status}). Cannot calculate round.`;
          console.warn(`DO (${this.match_do_id}): ${msg}`);
          return { success: false, message: msg };
      }
      // Check if there's a current song
      if (!this.matchData.current_song) {
           const msg = "No current song set for calculation.";
           console.error(`DO (${this.match_do_id}): ${msg}`);
           return { success: false, message: msg };
      }


      console.log(`DO (${this.match_do_id}) Calculating Round ${this.matchData.current_match_song_index + 1} with A: ${payload.teamA_percentage}%, B: ${payload.teamB_percentage}%`);

      // --- 1. Parse Damage & Get Professions ---
      // Validate percentages are numbers (already done in Worker, but double check)
      const teamAPercentage = typeof payload.teamA_percentage === 'number' ? payload.teamA_percentage : 0;
      const teamBPercentage = typeof payload.teamB_percentage === 'number' ? payload.teamB_percentage : 0;
      const teamAEffectValue = typeof payload.teamA_effect_value === 'number' ? payload.teamA_effect_value : 0;
      const teamBEffectValue = typeof payload.teamB_effect_value === 'number' ? payload.teamB_effect_value : 0;


      const teamADamageDigits = this.parseDamageDigits(teamAPercentage);
      const teamBDamageDigits = this.parseDamageDigits(teamBPercentage);
      let teamABaseDamage = teamADamageDigits.reduce((sum, digit) => sum + digit, 0);
      let teamBBaseDamage = teamBDamageDigits.reduce((sum, digit) => sum + digit, 0);

      const teamACurrentProfession = this.matchData.teamA_current_player_profession;
      const teamBCurrentProfession = this.matchData.teamB_current_player_profession;
      let teamAMaxDigitDamage = Math.max(0, ...teamADamageDigits);
      let teamBMaxDigitDamage = Math.max(0, ...teamBDamageDigits);

      console.log(`Base Damage - A: ${teamABaseDamage} (Digits: ${teamADamageDigits}), B: ${teamBBaseDamage} (Digits: ${teamBDamageDigits})`);
      console.log(`Professions - A: ${teamACurrentProfession}, B: ${teamBCurrentProfession}`);
      console.log(`Effect Values - A: ${teamAEffectValue}, B: ${teamBEffectValue}`);


      // --- 2. Apply Profession Effects (Pre-damage/Modification) ---
      let teamAModifiedDamageToB = teamABaseDamage; // Damage A will deal to B
      let teamBModifiedDamageToA = teamBBaseDamage; // Damage B will deal to A
      let teamAHealFromSupporterSkill = 0; // Supporter's *base* skill heal (converted damage)
      let teamBHealFromSupporterSkill = 0; // Supporter's *base* skill heal (converted damage)
      let teamAProfessionEffectLog = '';
      let teamBProfessionEffectLog = '';


      // Attacker: Additional damage
      if (teamACurrentProfession === 'attacker') {
          teamAModifiedDamageToB += teamAMaxDigitDamage;
          teamAProfessionEffectLog = `绝剑士技能：追加最高位数字伤害 ${teamAMaxDigitDamage}。`;
          console.log(`A Attacker skill adds ${teamAMaxDigitDamage} damage to B.`);
      }
      if (teamBCurrentProfession === 'attacker') {
          teamBModifiedDamageToA += teamBMaxDigitDamage;
          teamBProfessionEffectLog = `绝剑士技能：追加最高位数字伤害 ${teamBMaxDigitDamage}。`;
          console.log(`B Attacker skill adds ${teamBMaxDigitDamage} damage to A.`);
      }

      // Defender: Invalidate one random opponent damage digit
      if (teamACurrentProfession === 'defender' && teamBDamageDigits.length > 0) {
          const randomIndex = Math.floor(Math.random() * teamBDamageDigits.length);
          const invalidatedDamage = teamBDamageDigits[randomIndex];
          teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - invalidatedDamage); // Ensure damage doesn't go negative
          teamAProfessionEffectLog = `矩盾手技能：无效化对方随机一位数字伤害 ${invalidatedDamage}。`;
          console.log(`A Defender skill invalidates B's digit ${randomIndex + 1} (${invalidatedDamage} damage). B damage to A is now ${teamBModifiedDamageToA}.`);
      }
      if (teamBCurrentProfession === 'defender' && teamADamageDigits.length > 0) {
          const randomIndex = Math.floor(Math.random() * teamADamageDigits.length);
          const invalidatedDamage = teamADamageDigits[randomIndex];
          teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - invalidatedDamage); // Ensure damage doesn't go negative
          teamBProfessionEffectLog = `矩盾手技能：无效化对方随机一位数字伤害 ${invalidatedDamage}。`;
          console.log(`B Defender skill invalidates A's digit ${randomIndex + 1} (${invalidatedDamage} damage). A damage to B is now ${teamAModifiedDamageToB}.`);
      }

      // Supporter: Invalidate highest/lowest damage digits, convert to heal
      if (teamACurrentProfession === 'supporter' && teamADamageDigits.length >= 2) {
          const sortedDigits = [...teamADamageDigits].sort((a, b) => a - b);
          const lowest = sortedDigits[0];
          const highest = sortedDigits[sortedDigits.length - 1];
          const conversion = lowest + highest;
          teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - conversion);
          teamAHealFromSupporterSkill += conversion;
          teamAProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
          console.log(`A Supporter skill converts ${conversion} damage to heal. A damage to B is now ${teamAModifiedDamageToB}.`);
      }
      if (teamBCurrentProfession === 'supporter' && teamBDamageDigits.length >= 2) {
          const sortedDigits = [...teamBDamageDigits].sort((a, b) => a - b);
          const lowest = sortedDigits[0];
          const highest = sortedDigits[sortedDigits.length - 1];
          const conversion = lowest + highest;
          teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - conversion);
          teamBHealFromSupporterSkill += conversion;
          teamBProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
          console.log(`B Supporter skill converts ${conversion} damage to heal. B damage to A is now ${teamBModifiedDamageToA}.`);
      }
      console.log(`Modified Damage - A to B: ${teamAModifiedDamageToB}, B to A: ${teamBModifiedDamageToA}`);
      console.log(`Supporter Base Skill Heal Stored - A: ${teamAHealFromSupporterSkill}, B: ${teamBHealFromSupporterSkill}`);


      // --- 3. Apply Modified Damage & Calculate Potential Overflow ---
      let currentAHealth = this.matchData.teamA_score;
      let currentBHealth = this.matchData.teamB_score;

      // Calculate health *after* damage, and potential overflow if health drops below 0
      // These are the health values *before* any mirror effects apply
      let healthAfterDamageA = currentAHealth - teamBModifiedDamageToA;
      let healthAfterDamageB = currentBHealth - teamAModifiedDamageToB;

      // This is the "raw" overflow before any mirror considerations. Defender mirror uses this.
      let rawOverflowDamageToA = healthAfterDamageA < 0 ? Math.abs(healthAfterDamageA) : 0;
      let rawOverflowDamageToB = healthAfterDamageB < 0 ? Math.abs(healthAfterDamageB) : 0;

      console.log(`Health Before Damage - A: ${currentAHealth}, B: ${currentBHealth}`);
      console.log(`Health After Direct Damage (Potential) - A: ${healthAfterDamageA}, B: ${healthAfterDamageB}`);
      console.log(`Raw Overflow Damage - To A (by B): ${rawOverflowDamageToA}, To B (by A): ${rawOverflowDamageToB}`);

      // --- 4. Check and Apply Mirror Effect (with chain reaction) ---
      let teamAMirrorUsedThisTurn = false;
      let teamBMirrorUsedThisTurn = false;
      let teamAHealFromSupporterMirrorBonus = 0; // Supporter's *mirror bonus* heal
      let teamBHealFromSupporterMirrorBonus = 0; // Supporter's *mirror bonus* heal
      let teamAReflectedDamageByDefenderMirror = 0; // Track reflected damage for summary
      let teamBReflectedDamageByDefenderMirror = 0; // Track reflected damage for summary
      let teamAAttackerMirrorExtraDamage = 0; // Track attacker extra damage for summary
      let teamBAttackerMirrorExtraDamage = 0; // Track attacker extra damage for summary
      let teamAMirrorEffectLog = '';
      let teamBMirrorEffectLog = '';


      // Health variables that will be modified by mirrors and subsequent effects
      // Start with health after initial damage
      let finalHealthA = healthAfterDamageA;
      let finalHealthB = healthAfterDamageB;

      const canAInitiallyTriggerMirror = finalHealthA <= 0 && this.matchData.teamA_mirror_available;
      const canBInitiallyTriggerMirror = finalHealthB <= 0 && this.matchData.teamB_mirror_available;

      // RULE: Both trigger mirror simultaneously
      if (canAInitiallyTriggerMirror && canBInitiallyTriggerMirror) {
          console.log("RULE: Both teams trigger Mirror simultaneously.");
          this.matchData.teamA_mirror_available = false;
          this.matchData.teamB_mirror_available = false;
          teamAMirrorUsedThisTurn = true;
          teamBMirrorUsedThisTurn = true;
          finalHealthA = MIRROR_HEALTH_RESTORE; // Both set to 20
          finalHealthB = MIRROR_HEALTH_RESTORE; // Both set to 20
          teamAMirrorEffectLog = '双方同时触发复影折镜，血量恢复至20。';
          teamBMirrorEffectLog = '双方同时触发复影折镜，血量恢复至20。';
          console.log(`Both Mirrors consumed. A Health: ${finalHealthA}, B Health: ${finalHealthB}. No profession mirror effects will apply.`);
          // IMPORTANT: No profession-specific mirror effects (Attacker extra, Defender reflect, Supporter mirror bonus)
          // Supporter base skill heal will still apply later.
      } else {
          // Not simultaneous, handle potential individual and chain triggers

          // --- Pass 1: A's Initial Mirror Trigger ---
          // Check if A triggers based on initial damage AND A's mirror is available
          if (canAInitiallyTriggerMirror) {
              console.log("Team A triggers Mirror (Pass 1).");
              this.matchData.teamA_mirror_available = false;
              teamAMirrorUsedThisTurn = true;
              finalHealthA = MIRROR_HEALTH_RESTORE; // A's health restored
              teamAMirrorEffectLog = '触发复影折镜，血量恢复至20。';
              console.log(`A Mirror consumed. A Health set to ${finalHealthA}.`);

              // Apply A's profession-specific Mirror effect
              if (teamACurrentProfession === 'attacker') {
                  teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage;
                  finalHealthB -= teamAAttackerMirrorExtraDamage; // Damage B
                  teamAMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamAAttackerMirrorExtraDamage}。`;
                  console.log(`A Attacker Mirror adds ${teamAAttackerMirrorExtraDamage} damage to B. B health now potentially ${finalHealthB}.`);
              } else if (teamACurrentProfession === 'defender') {
                  teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA; // A's Defender reflects B's *original* overflow to A
                  finalHealthB -= teamAReflectedDamageByDefenderMirror;
                  teamAMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamAReflectedDamageByDefenderMirror}。`;
                  console.log(`A Defender Mirror reflects ${teamAReflectedDamageByDefenderMirror} (original overflow B caused to A) back to B. B health now potentially ${finalHealthB}.`);
              } else if (teamACurrentProfession === 'supporter') {
                  teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill; // Supporter mirror *bonus* is equal to base skill heal
                  teamAMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamAHealFromSupporterMirrorBonus}。`;
                  console.log(`A Supporter Mirror will grant a bonus heal of ${teamAHealFromSupporterMirrorBonus}.`);
              }
          }

          // --- Pass 2: B's Mirror Trigger (considering A's Pass 1 effect) ---
          // Check if B triggers based on health *after* A's Pass 1 effect AND B's mirror is available
          const canBTriggerAfterAPass1 = finalHealthB <= 0 && this.matchData.teamB_mirror_available && !teamBMirrorUsedThisTurn;
          if (canBTriggerAfterAPass1) {
              console.log("Team B triggers Mirror (Pass 2 - potentially after A's Pass 1 effect).");
              this.matchData.teamB_mirror_available = false;
              teamBMirrorUsedThisTurn = true;
              finalHealthB = MIRROR_HEALTH_RESTORE; // B's health restored
              teamBMirrorEffectLog = '触发复影折镜，血量恢复至20。';
              console.log(`B Mirror consumed. B Health set to ${finalHealthB}.`);

              // Apply B's profession-specific Mirror effect
              if (teamBCurrentProfession === 'attacker') {
                  teamBAttackerMirrorExtraDamage = teamBMaxDigitDamage;
                  finalHealthA -= teamBAttackerMirrorExtraDamage; // Damage A
                  teamBMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamBAttackerMirrorExtraDamage}。`;
                  console.log(`B Attacker Mirror adds ${teamBAttackerMirrorExtraDamage} damage to A. A health now potentially ${finalHealthA}.`);
              } else if (teamBCurrentProfession === 'defender') {
                  teamBReflectedDamageByDefenderMirror = rawOverflowDamageToB; // B's Defender reflects A's *original* overflow to B (Corrected)
                  finalHealthA -= teamBReflectedDamageByDefenderMirror;
                  teamBMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamBReflectedDamageByDefenderMirror}。`;
                  console.log(`B Defender Mirror reflects ${teamBReflectedDamageByDefenderMirror} (original overflow A caused to B) back to A. A health now potentially ${finalHealthA}.`);
              } else if (teamBCurrentProfession === 'supporter') {
                  teamBHealFromSupporterMirrorBonus = teamBHealFromSupporterSkill; // Supporter mirror *bonus*
                  teamBMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamBHealFromSupporterMirrorBonus}。`;
                  console.log(`B Supporter Mirror will grant a bonus heal of ${teamBHealFromSupporterMirrorBonus}.`);
              }
          }

          // --- Pass 3: A's Chain Reaction Mirror Trigger (if B's Pass 2 effect triggered A's mirror) ---
          // This happens ONLY IF A's mirror was NOT used in Pass 1 (i.e., !teamAMirrorUsedThisTurn was true at the start of Pass 1)
          // AND A's mirror is still available (which it is, because it wasn't used in Pass 1)
          // AND B's Pass 2 effect caused A's health (finalHealthA) to drop to <= 0.
          const canATriggerAfterBPass2 = finalHealthA <= 0 && this.matchData.teamA_mirror_available && !teamAMirrorUsedThisTurn;
          if (canATriggerAfterBPass2) {
              console.log("Team A triggers Mirror (Pass 3 - CHAIN REACTION after B's Pass 2 effect).");
              this.matchData.teamA_mirror_available = false; // Consume A's mirror now
              teamAMirrorUsedThisTurn = true; // Mark A's mirror as used
              finalHealthA = MIRROR_HEALTH_RESTORE; // A's health restored
              teamAMirrorEffectLog = '触发复影折镜 (连锁反应)，血量恢复至20。'; // Overwrite Pass 1 log
              console.log(`A Mirror consumed (Chain Reaction). A Health set to ${finalHealthA}.`);

              // Apply A's profession-specific Mirror effect (this is A's *only* mirror effect application)
              if (teamACurrentProfession === 'attacker') {
                  teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage; // Re-set/confirm bonus damage
                  finalHealthB -= teamAAttackerMirrorExtraDamage; // Damage B (B might have already used its mirror)
                  teamAMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamAAttackerMirrorExtraDamage}。`;
                  console.log(`A Attacker Mirror (Chain Reaction) adds ${teamAAttackerMirrorExtraDamage} damage to B. B health now potentially ${finalHealthB}.`);
              } else if (teamACurrentProfession === 'defender') {
                  // If A is defender, it reflects B's overflow.
                  // rawOverflowDamageToA is B's *initial* overflow to A.
                  // If B's mirror effect (e.g. attacker) caused new "overflow" to A, that's not what's reflected.
                  // Defender reflects the *opponent's initial attack's overflow*.
                  teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA;
                  finalHealthB -= teamAReflectedDamageByDefenderMirror;
                  teamAMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamAReflectedDamageByDefenderMirror}。`;
                  console.log(`A Defender Mirror (Chain Reaction) reflects ${teamAReflectedDamageByDefenderMirror} (original overflow B caused to A) back to B. B health now potentially ${finalHealthB}.`);
              } else if (teamACurrentProfession === 'supporter') {
                  // If A's mirror is used here for the first time, set the bonus.
                  // If it was already set (e.g. if this chain logic was more complex), ensure it's not doubled.
                  // Since it's a single use, this is fine.
                  teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill;
                  teamAMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamAHealFromSupporterMirrorBonus}。`;
                  console.log(`A Supporter Mirror (Chain Reaction) will grant a bonus heal of ${teamAHealFromSupporterMirrorBonus}.`);
              }
          }
      } // End of non-simultaneous mirror logic

      // If mirror was NOT used by a team, their health is what it was after initial damage.
      // This step is implicitly handled by initializing finalHealthA/B with healthAfterDamageA/B
      // and only changing them if a mirror triggers.

      console.log(`Health After Mirror Logic (incl. chain) - A: ${finalHealthA}, B: ${finalHealthB}`);

      // --- 5. Apply Supporter Heal ---
      // This includes the base skill heal AND any mirror bonus heal (if mirror triggered individually/chained)
      // If simultaneous mirror, mirror bonus is 0.
      finalHealthA += (teamAHealFromSupporterSkill + teamAHealFromSupporterMirrorBonus);
      finalHealthB += (teamBHealFromSupporterSkill + teamBHealFromSupporterMirrorBonus);
      console.log(`Supporter Total Heal Applied - A: ${teamAHealFromSupporterSkill + teamAHealFromSupporterMirrorBonus}, B: ${teamBHealFromSupporterSkill + teamBHealFromSupporterMirrorBonus}`);
      console.log(`Health After Supporter Heal - A: ${finalHealthA}, B: ${finalHealthB}`);

      // --- 6. Apply Effect Values (小分调整) ---
      finalHealthA += teamAEffectValue;
      finalHealthB += teamBEffectValue;
      console.log(`Health After Effect Values - A: ${finalHealthA}, B: ${finalHealthB}`);


      // --- 7. Update Match State and Check Match End Condition ---
      const healthBeforeRoundingA = finalHealthA; // Store pre-rounded for summary
      const healthBeforeRoundingB = finalHealthB; // Store pre-rounded for summary

      this.matchData.teamA_score = Math.round(finalHealthA); // Round to nearest integer for health
      this.matchData.teamB_score = Math.round(finalHealthB); // Round to nearest integer for health

      const aDead = this.matchData.teamA_score <= 0;
      const bDead = this.matchData.teamB_score <= 0;

      let newStatus: MatchState['status'];
      let matchEnded = false;

      if (aDead && bDead) {
          // Both are dead. Determine winner based on final score.
          if (this.matchData.teamA_score > this.matchData.teamB_score) {
              newStatus = 'team_A_wins'; // A has higher (less negative) score
          } else if (this.matchData.teamB_score > this.matchData.teamA_score) {
              newStatus = 'team_B_wins'; // B has higher (less negative) score
          } else {
              newStatus = 'draw_pending_resolution'; // Scores are equal (e.g., both 0 or both -5)
          }
          matchEnded = true;
          console.log(`Both teams <= 0. A: ${this.matchData.teamA_score}, B: ${this.matchData.teamB_score}. Status: ${newStatus}`);
      } else if (aDead) {
          newStatus = 'team_B_wins';
          matchEnded = true;
          console.log(`Team A <= 0. Status: ${newStatus}`);
      } else if (bDead) {
          newStatus = 'team_A_wins';
          matchEnded = true;
          console.log(`Team B <= 0. Status: ${newStatus}`);
      } else {
          // Neither team is dead
          // Check if this was the last standard round (BO6)
          if (this.matchData.current_match_song_index >= STANDARD_ROUNDS_COUNT - 1) { // 0-based index, so index 5 is the 6th song
               newStatus = 'tiebreaker_pending_song'; // Standard rounds finished, need tiebreaker
               console.log(`Standard rounds (${STANDARD_ROUNDS_COUNT}) finished, neither team is dead. Status: ${newStatus}`);
          } else {
               newStatus = 'round_finished'; // Not the last standard round, continue
               console.log(`Neither team <= 0. Round ${this.matchData.current_match_song_index + 1} finished. Status: ${newStatus}`);
          }
      }
      this.matchData.status = newStatus;
      console.log(`Final Health - A: ${this.matchData.teamA_score}, B: ${this.matchData.teamB_score}. New Status: ${this.matchData.status}`);

      // --- 8. Update Current Song Status and Details ---
      if (this.matchData.current_song) {
           this.matchData.current_song.status = 'completed';
           this.matchData.current_song.teamA_player_id = this.matchData.teamA_current_player_id ?? undefined;
           this.matchData.current_song.teamB_player_id = this.matchData.teamB_current_player_id ?? undefined;
           this.matchData.current_song.teamA_percentage = teamAPercentage;
           this.matchData.current_song.teamB_percentage = teamBPercentage;
           // Calculate damage dealt based on health change
           this.matchData.current_song.teamA_damage_dealt = currentBHealth - finalHealthB; // Damage A dealt to B
           this.matchData.current_song.teamB_damage_dealt = currentAHealth - finalHealthA; // Damage B dealt to A
           this.matchData.current_song.teamA_effect_value = teamAEffectValue;
           this.matchData.current_song.teamB_effect_value = teamBEffectValue;
           this.matchData.current_song.teamA_health_after = this.matchData.teamA_score;
           this.matchData.current_song.teamB_health_after = this.matchData.teamB_score;
           this.matchData.current_song.teamA_mirror_triggered = teamAMirrorUsedThisTurn;
           this.matchData.current_song.teamB_mirror_triggered = teamBMirrorUsedThisTurn;
      }


      // --- 9. Prepare Round Summary ---
      const summary: RoundSummary = {
          round_number_in_match: this.matchData.current_match_song_index + 1,
          song_id: this.matchData.current_song?.song_id ?? -1,
          song_title: this.matchData.current_song?.song_title ?? '未知歌曲',
          selected_difficulty: this.matchData.current_song?.song_difficulty ?? '未知难度',

          teamA_player_id: this.matchData.teamA_current_player_id ?? -1,
          teamB_player_id: this.matchData.teamB_current_player_id ?? -1,
          teamA_player_nickname: this.matchData.teamA_current_player_nickname,
          teamB_player_nickname: this.matchData.teamB_current_player_nickname,

          teamA_percentage: teamAPercentage,
          teamB_percentage: teamBPercentage,
          teamA_effect_value_applied: teamAEffectValue,
          teamB_effect_value_applied: teamBEffectValue,

          teamA_damage_digits: teamADamageDigits,
          teamB_damage_digits: teamBDamageDigits,
          teamA_base_damage: teamABaseDamage,
          teamB_base_damage: teamBBaseDamage,
          teamA_profession: teamACurrentProfession,
          teamB_profession: teamBCurrentProfession,
          teamA_profession_effect_applied: teamAProfessionEffectLog,
          teamB_profession_effect_applied: teamBProfessionEffectLog,
          teamA_modified_damage_to_B: teamAModifiedDamageToB,
          teamB_modified_damage_to_A: teamBModifiedDamageToA,

          teamA_health_before_round: currentAHealth,
          teamB_health_before_round: currentBHealth,

          teamA_mirror_triggered: teamAMirrorUsedThisTurn,
          teamB_mirror_triggered: teamBMirrorUsedThisTurn,
          teamA_mirror_effect_applied: teamAMirrorEffectLog,
          teamB_mirror_effect_applied: teamBMirrorEffectLog,
          teamA_supporter_base_skill_heal: teamAHealFromSupporterSkill,
          teamB_supporter_base_skill_heal: teamBHealFromSupporterSkill,
          teamA_supporter_mirror_bonus_heal: teamAHealFromSupporterMirrorBonus,
          teamB_supporter_mirror_bonus_heal: teamBHealFromSupporterMirrorBonus,

          teamA_final_damage_dealt: currentBHealth - finalHealthB, // Damage A caused to B
          teamB_final_damage_dealt: currentAHealth - finalHealthA, // Damage B caused to A

          teamA_health_change: this.matchData.teamA_score - currentAHealth,
          teamB_health_change: this.matchData.teamB_score - currentBHealth,
          teamA_health_after: this.matchData.teamA_score,
          teamB_health_after: this.matchData.teamB_score,

          is_tiebreaker_song: this.matchData.current_song?.is_tiebreaker_song ?? false,

          log: [], // Add detailed logs here if needed
      };
      this.matchData.roundSummary = summary; // Store it in the state
      console.log("Round Summary:", JSON.stringify(summary, null, 2));


      // --- 10. Save State and Broadcast ---
      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);

          // If match ended, close websockets
          if (matchEnded || this.matchData.status === 'archived') {
              this.websockets.forEach(ws => ws.close(1000, `Match ended. Status: ${this.matchData.status}`));
              this.websockets = [];
          }
          return { success: true, message: `Round ${this.matchData.current_match_song_index + 1} calculated. New status: ${this.matchData.status}`, roundSummary: summary };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) Failed to save state after calculation:`, e);
          return { success: false, message: `Failed to save state after calculation: ${e.message}` };
      }
  }


  // Archive the current round's data to D1 match_rounds_history table
  // This is primarily called automatically by nextRound or archiveMatch
  private async archiveCurrentRound(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
      if (!this.matchData || !this.matchData.roundSummary || !this.matchData.current_song) {
          return { success: false, message: "No match data, round summary, or current song to archive round." };
      }
      if (this.matchData.status === 'archived') {
          return { success: false, message: "Match is already archived, cannot archive rounds." };
      }

      const summary = this.matchData.roundSummary;
      const currentSong = this.matchData.current_song;

      try {
          const stmt = this.env.DB.prepare(
              `INSERT INTO match_rounds_history (
                  tournament_match_id, match_do_id, round_number_in_match,
                  song_id, selected_difficulty, picker_team_id, picker_member_id,
                  team1_member_id, team2_member_id, team1_percentage, team2_percentage,
                  team1_damage_dealt, team2_damage_dealt, team1_health_change, team2_health_change,
                  team1_health_before, team2_health_before, team1_health_after, team2_health_after,
                  team1_mirror_triggered, team2_mirror_triggered, team1_effect_value, team2_effect_value,
                  is_tiebreaker_song, recorded_at, round_summary_json
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tournament_match_id, round_number_in_match) DO UPDATE SET
                  song_id = excluded.song_id,
                  selected_difficulty = excluded.selected_difficulty,
                  picker_team_id = excluded.picker_team_id,
                  picker_member_id = excluded.picker_member_id,
                  team1_member_id = excluded.team1_member_id,
                  team2_member_id = excluded.team2_member_id,
                  team1_percentage = excluded.team1_percentage,
                  team2_percentage = excluded.team2_percentage,
                  team1_damage_dealt = excluded.team1_damage_dealt,
                  team2_damage_dealt = excluded.team2_damage_dealt,
                  team1_health_change = excluded.team1_health_change,
                  team2_health_change = excluded.team2_health_change,
                  team1_health_before = excluded.team1_health_before,
                  team2_health_before = excluded.team2_health_before,
                  team1_health_after = excluded.team1_health_after,
                  team2_health_after = excluded.team2_health_after,
                  team1_mirror_triggered = excluded.team1_mirror_triggered,
                  team2_mirror_triggered = excluded.team2_mirror_triggered,
                  team1_effect_value = excluded.team1_effect_value,
                  team2_effect_value = excluded.team2_effect_value,
                  is_tiebreaker_song = excluded.is_tiebreaker_song,
                  recorded_at = excluded.recorded_at,
                  round_summary_json = excluded.round_summary_json
              `
          );

          const result = await stmt.bind(
              this.matchData.tournament_match_id,
              this.match_do_id,
              summary.round_number_in_match, // Use round_number_in_match from summary (1-based)
              summary.song_id,
              summary.selected_difficulty, // Use difficulty from summary (e.g., 'M 13')
              currentSong.picker_team_id,
              currentSong.picker_member_id,
              summary.teamA_player_id,
              summary.teamB_player_id,
              summary.teamA_percentage,
              summary.teamB_percentage,
              summary.teamA_final_damage_dealt, // Use final damage from summary
              summary.teamB_final_damage_dealt,
              summary.teamA_health_change,
              summary.teamB_health_change,
              summary.teamA_health_before_round,
              summary.teamB_health_before_round,
              summary.teamA_health_after,
              summary.teamB_health_after,
              summary.teamA_mirror_triggered ? 1 : 0,
              summary.teamB_mirror_triggered ? 1 : 0,
              summary.teamA_effect_value_applied,
              summary.teamB_effect_value_applied,
              summary.is_tiebreaker_song ? 1 : 0,
              new Date().toISOString(),
              JSON.stringify(summary)
          ).run();

          if (result.success) {
              console.log(`DO (${this.match_do_id}) Round ${summary.round_number_in_match} data archived/updated in D1 match_rounds_history.`);
              return { success: true, message: `Round ${summary.round_number_in_match} archived.`, d1RecordId: result.meta.last_row_id };
          } else {
              console.error(`DO (${this.match_do_id}) failed to archive round ${summary.round_number_in_match} to D1:`, result.error);
              return { success: false, message: `Failed to archive round: ${result.error}` };
          }

      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) exception during D1 round archive:`, e);
          return { success: false, message: `Exception during round archive: ${e.message}` };
      }
  }

  // Advance to the next round
  private async nextRound(): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to advance round." };
      }
      // Only allowed if the previous round is finished and not in a final state
      if (this.matchData.status !== 'round_finished') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'round_finished' to advance.` };
      }

      // Automatically archive the current round before advancing
      const archiveResult = await this.archiveCurrentRound();
      if (!archiveResult.success) {
          console.warn(`DO (${this.match_do_id}) Failed to auto-archive current round ${this.matchData.current_match_song_index + 1} before advancing:`, archiveResult.message);
          // Decide if you want to stop here or proceed anyway
          // For now, we proceed but log the warning.
      }

      const nextSongIndex = this.matchData.current_match_song_index + 1;

      // Check if there is a next song in the list
      if (nextSongIndex >= this.matchData.match_song_list.length) {
           // This case should ideally be handled by calculateRoundOutcome transitioning to a final state
           // or 'tiebreaker_pending_song' after the last standard round.
           // If we reach here, it means nextRound was called incorrectly or logic error.
           const msg = `Cannot advance round. No song found at index ${nextSongIndex}. Match song list length: ${this.matchData.match_song_list.length}.`;
           console.error(`DO (${this.match_do_id}): ${msg}`);
           // Transition to a state indicating an issue or end of list if not already
           if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'tiebreaker_pending_song', 'archived'].includes(this.matchData.status)) {
                this.matchData.status = 'completed'; // Or a specific error status
                await this.state.storage.put('matchData', this.matchData);
                this.broadcast(this.matchData);
           }
           return { success: false, message: msg };
      }

      this.matchData.current_match_song_index = nextSongIndex;
      this.matchData.current_song = this.matchData.match_song_list[nextSongIndex];
      this.matchData.current_song.status = 'ongoing'; // Mark the new song as ongoing

      // Determine players for the next round based on the new index and cycling order
      const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
      const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
      const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

      this.matchData.teamA_current_player_id = playerAId;
      this.matchData.teamB_current_player_id = playerBId;
      this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
      this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
      this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
      this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);


      this.matchData.roundSummary = null; // Clear summary for the new round
      this.matchData.status = 'pending_scores'; // Reset status to pending for the new round

      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);
          console.log(`DO (${this.match_do_id}) advanced to Round ${this.matchData.current_match_song_index + 1}`);
          return { success: true, message: `Advanced to Round ${this.matchData.current_match_song_index + 1}` };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) failed to advance round:`, e);
          return { success: false, message: `Failed to advance round: ${e.message}` };
      }
  }

  // Staff selects a tiebreaker song
  private async selectTiebreakerSong(payload: SelectTiebreakerSongPayload): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to select tiebreaker song." };
      }
      if (this.matchData.status !== 'tiebreaker_pending_song') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'tiebreaker_pending_song' to select tiebreaker.` };
      }

      console.log(`DO (${this.match_do_id}) Staff selecting tiebreaker song: ${payload.song_id} (${payload.selected_difficulty})`);

      try {
          // 1. Fetch song details from D1
          const song = await this.env.DB.prepare("SELECT * FROM songs WHERE id = ?").bind(payload.song_id).first<Song>();
          if (!song) {
              return { success: false, message: `Song with ID ${payload.song_id} not found.` };
          }
          const parsedLevels: SongLevel = song.levels_json ? JSON.parse(song.levels_json) : {};
          const difficultyValue = parsedLevels[payload.selected_difficulty as keyof SongLevel] || '??'; // Get the difficulty value (e.g., "13")
          const fullDifficultyString = `${payload.selected_difficulty} ${difficultyValue}`; // e.g., "M 13"

          // 2. Create a new MatchSong object for the tiebreaker
          const tiebreakerSong: MatchSong = {
              song_id: song.id,
              song_title: song.title,
              song_difficulty: fullDifficultyString, // Store the combined string
              song_element: song.category === 'original' ? 'fire' : song.category === 'niconico' ? 'wood' : null, // Example mapping
              cover_filename: song.cover_filename,
              bpm: song.bpm,
              fullCoverUrl: song.cover_filename ? `https://${this.env.SONG_COVER_BUCKET.name}/${song.cover_filename}` : undefined, // Assuming SONG_COVER_BUCKET binding

              // Assign picker info (can use a special ID for Staff/System)
              // TODO: Define a Staff/System member ID or team ID for tiebreakers
              picker_member_id: -1, // Placeholder for Staff/System
              picker_team_id: -1, // Placeholder for Staff/System
              is_tiebreaker_song: true,

              status: 'pending', // Will be set to 'ongoing' when the round starts
          };

          // 3. Add the tiebreaker song to the match song list
          this.matchData.match_song_list.push(tiebreakerSong);

          // 4. Update state to point to the new song
          this.matchData.current_match_song_index = this.matchData.match_song_list.length - 1;
          this.matchData.current_song = tiebreakerSong;
          this.matchData.current_song.status = 'ongoing'; // Mark as ongoing

          // 5. Determine players for the tiebreaker round
          // TODO: Define tiebreaker player selection rule (e.g., cycle continues, or specific players?)
          // For now, let's assume cycling continues based on the new index
          const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
          const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
          const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

          this.matchData.teamA_current_player_id = playerAId;
          this.matchData.teamB_current_player_id = playerBId;
          this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
          this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
          this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
          this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);


          this.matchData.roundSummary = null; // Clear summary
          this.matchData.status = 'pending_scores'; // Transition back to pending scores

          // 6. Save state and broadcast
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);

          console.log(`DO (${this.match_do_id}) Tiebreaker song selected. Advanced to Round ${this.matchData.current_match_song_index + 1}`);
          return { success: true, message: `Tiebreaker song selected. Advanced to Round ${this.matchData.current_match_song_index + 1}` };

      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) exception selecting tiebreaker song:`, e);
          return { success: false, message: `Failed to select tiebreaker song: ${e.message}` };
      }
  }


  // Archive the entire match summary and rounds to D1
  private async archiveMatch(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to archive match." };
      }
      if (this.matchData.status === 'archived') { // Prevent re-archiving
          return { success: true, message: "Match already archived.", d1RecordId: this.match_do_id };
      }
      // Only allow archiving if the match has reached a final state or is explicitly commanded (e.g., by admin)
      if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'completed'].includes(this.matchData.status)) {
           // Allow archiving from any state if explicitly called? Or require a specific admin flag?
           // For now, let's allow archiving from any state if the internal method is called,
           // but the Worker endpoint should enforce status checks.
           console.warn(`DO (${this.match_do_id}): Archiving match from non-final state: ${this.matchData.status}`);
      }


      try {
          // 1. Archive all completed rounds to match_rounds_history
          const roundsToArchive = this.matchData.match_song_list.filter(song => song.status === 'completed');
          const archiveRoundPromises = roundsToArchive.map(async (song, index) => {
               // Need to reconstruct RoundSummary if not stored per song, or fetch from DO storage if stored separately
               // Assuming RoundSummary is stored in matchData.roundSummary for the *last* round calculated.
               // If you need history for *all* rounds, you need to store RoundSummary per song in match_song_list
               // or fetch from match_rounds_history table itself if it was auto-archived per round.

               // Let's assume archiveCurrentRound handles saving the *last* round's summary.
               // For previous rounds, we might only have the data stored in the MatchSong object itself.
               // A better approach: archiveCurrentRound is called *after* each round calculation.
               // So when archiveMatch is called, all rounds should already be in match_rounds_history.
               // We just need to update the tournament_matches table.

               // Re-archive the last round just in case it wasn't auto-archived
               if (this.matchData.roundSummary && this.matchData.roundSummary.round_number_in_match === this.matchData.current_match_song_index + 1) {
                    await this.archiveCurrentRound(); // This handles ON CONFLICT, so safe to call again
               }
               // If you need to ensure *all* rounds are archived here, you'd need to iterate through match_song_list
               // and call archiveCurrentRound for each completed song, potentially fetching old state/summary if not in DO state.
               // This is complex. Let's stick to the model where archiveCurrentRound is called after each calculation.
          });
          await Promise.all(archiveRoundPromises); // Wait for any pending round archives


          // 2. Update the corresponding tournament_matches entry
          if (this.matchData.tournament_match_id) {
              try {
                  // Determine the winner team ID based on final scores
                  const winnerTeamId = this.determineWinnerTeamId(this.matchData);

                  // Determine the status for the tournament_matches table
                  const tournamentMatchStatus = ['team_A_wins', 'team_B_wins'].includes(this.matchData.status) ? 'completed'
                                                  : this.matchData.status === 'draw_pending_resolution' ? 'completed' // Or 'draw' if you add that status
                                                  : 'archived'; // Use 'archived' for manual archives or other final states

                  const updateTournamentStmt = this.env.DB.prepare(
                      `UPDATE tournament_matches SET
                         status = ?,
                         winner_team_id = ?,
                         final_score_team1 = ?,
                         final_score_team2 = ?,
                         match_do_id = ?, -- Keep the DO ID linked
                         updated_at = ?
                       WHERE id = ?`
                  );
                  const updateTournamentResult = await updateTournamentStmt.bind(
                      tournamentMatchStatus,
                      winnerTeamId,
                      this.matchData.teamA_score,
                      this.matchData.teamB_score,
                      this.match_do_id,
                      new Date().toISOString(),
                      this.matchData.tournament_match_id
                  ).run();

                  if (!updateTournamentResult.success) {
                      console.error(`DO (${this.match_do_id}) failed to update tournament_matches entry ${this.matchData.tournament_match_id}:`, updateTournamentResult.error);
                      // This is a secondary failure. Log and continue.
                  } else {
                      console.log(`DO (${this.match_do_id}) updated tournament_matches entry ${this.matchData.tournament_match_id} status to '${tournamentMatchStatus}'.`);
                  }

              } catch (e: any) {
                  console.error(`DO (${this.match_do_id}) exception during tournament_matches update:`, e);
                  // Log the exception
              }
          }


          // 3. Update DO's internal state to reflect the whole match is archived
          this.matchData.status = 'archived'; // DO's internal archived status
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData); // Notify clients about the archival status

          // 4. Close WebSockets as the live match is over
          this.websockets.forEach(ws => ws.close(1000, "Match archived and finished."));
          this.websockets = [];

          console.log(`DO (${this.match_do_id}) Match archived.`);
          return { success: true, message: "Match data archived to D1.", d1RecordId: this.match_do_id };


      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) exception during D1 match archive:`, e);
          return { success: false, message: `Exception during match archive: ${e.message}` };
      }
  }

  // Resolve a draw by setting the winner
  private async resolveDraw(winnerDesignation: 'teamA' | 'teamB'): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to resolve draw." };
      }
      if (this.matchData.status !== 'draw_pending_resolution') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'draw_pending_resolution' to resolve.` };
      }

      console.log(`DO (${this.match_do_id}) Resolving draw. Winner: ${winnerDesignation}`);

      // Set the winner status
      if (winnerDesignation === 'teamA') {
          this.matchData.status = 'team_A_wins';
          // Optionally adjust loser's score to be clearly <= 0 if needed for display/archive
          if (this.matchData.teamB_score > 0) this.matchData.teamB_score = 0;
      } else if (winnerDesignation === 'teamB') {
          this.matchData.status = 'team_B_wins';
          // Optionally adjust loser's score
          if (this.matchData.teamA_score > 0) this.matchData.teamA_score = 0;
      } else {
          return { success: false, message: "Invalid winner designation." };
      }

      // Automatically archive the match after resolving the draw
      const archiveResult = await this.archiveMatch();
      if (!archiveResult.success) {
          console.error(`DO (${this.match_do_id}) Failed to auto-archive match after draw resolution:`, archiveResult.message);
          // Decide if you want to revert status or just log error
          // For now, we'll proceed but return the archive error message
          return { success: false, message: `Draw resolved, but failed to archive match: ${archiveResult.message}` };
      }

      console.log(`DO (${this.match_do_id}) Draw resolved. New status: ${this.matchData.status}`);
      // State is already saved and broadcast by archiveMatch
      return { success: true, message: `Draw resolved. ${this.matchData.status.replace('_', ' ')}.` };
  }


  // Start a new match by resetting the DO state (less preferred, use initializeFromSchedule)
  // This method might not be needed anymore if all matches start from schedule
  /*
  private async newMatch(): Promise<{ success: boolean; message?: string }> {
     // Only allow starting a new match if the current one is archived
     if (this.matchData?.status !== 'archived') {
         return { success: false, message: "Current match must be archived before starting a new one." };
     }

    try {
      // Clear all state stored for this DO instance
      await this.state.storage.deleteAll();
      console.log(`DO (${this.match_do_id}) storage cleared.`);

      // Initialize with default state for the new match (no associated tournament match)
      // This default state is minimal now, expecting initializeFromSchedule
      this.matchData = {
           match_do_id: this.match_do_id,
           tournament_match_id: -1, // Indicate not linked yet
           status: 'scheduled', // Or 'uninitialized'
           round_name: '未知轮次',
           current_match_song_index: 0,
           teamA_id: -1, teamB_id: -1,
           teamA_name: '未知队伍A', teamB_name: '未知队伍B',
           teamA_score: INITIAL_HEALTH, teamB_score: INITIAL_HEALTH,
           teamA_player_order_ids: [], teamB_player_order_ids: [],
           teamA_current_player_id: null, teamB_current_player_id: null,
           teamA_current_player_nickname: '未知选手', teamB_current_player_nickname: '未知选手',
           teamA_current_player_profession: null, teamB_current_player_profession: null,
           teamA_mirror_available: true, teamB_mirror_available: true,
           match_song_list: [],
           current_song: null,
           roundSummary: null,
      };
      await this.state.storage.put('matchData', this.matchData);
      console.log(`DO (${this.match_do_id}) initialized for new match (minimal).`);

      // Broadcast the new state (clients will see a reset)
      this.broadcast(this.matchData);

      // Note: WebSockets were closed during archiveMatch. Clients will need to reconnect.

      return { success: true, message: "New match started." };
    } catch (e: any) {
      console.error(`DO (${this.match_do_id}) failed to start new match:`, e);
      return { success: false, message: `Failed to start new match: ${e.message}` };
    }
  }
  */


  async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);

      // Ensure matchData is loaded (it should be by the constructor's blockConcurrencyWhile)
      // This check is a safeguard, constructor should handle initial load
      // If matchData is still null after constructor, something is wrong.
      // However, for a brand new DO instance, matchData might be the minimal state
      // set in the constructor if initializeFromSchedule hasn't been called yet.
      // We should allow initializeFromSchedule even if matchData is minimal.
      // For other actions, matchData must be properly initialized.

      // WebSocket upgrade
      if (url.pathname === '/websocket') { // Internal path for WS
          if (request.headers.get('Upgrade') !== 'websocket') {
              return new Response('Expected Upgrade: websocket', { status: 426 });
          }
          const [client, server] = Object.values(new WebSocketPair());
          this.websockets.push(server);
          server.accept();
          console.log(`DO (${this.match_do_id}) WebSocket connected. Total: ${this.websockets.length}`);

          // Send current state immediately upon connection if initialized
          if (this.matchData) {
               server.send(JSON.stringify(this.matchData));
          } else {
               // Should not happen if constructor works, but as safeguard
               server.send(JSON.stringify({ success: false, error: "Match data not initialized in DO" }));
          }


          // Handle messages from this specific client (optional, e.g., for pings)
          server.addEventListener('message', event => {
              console.log(`DO (${this.match_do_id}) WS message from client:`, event.data);
              // server.send(`Echo: ${event.data}`); // Example echo
          });
          server.addEventListener('close', (event) => {
              console.log(`DO (${this.match_do_id}) WebSocket closed. Code: ${event.code}, Reason: ${event.reason}`);
              this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
              console.log(`DO (${this.match_do_id}) WebSocket disconnected. Remaining: ${this.websockets.length}`);
          });
          server.addEventListener('error', (err) => {
              console.error(`DO (${this.match_do_id}) WebSocket error:`, err);
              this.websockets = this.websockets.filter(ws => ws !== server);
              console.log(`DO (${this.match_do_id}) WebSocket error disconnected. Remaining: ${this.websockets.length}`);
          });
          return new Response(null, { status: 101, webSocket: client });
      }

      // Get current state
      if (url.pathname === '/state' && request.method === 'GET') { // Internal path
           if (!this.matchData) {
               return new Response(JSON.stringify({ success: false, error: "Match data not initialized in DO" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
           }
          return new Response(JSON.stringify(this.matchData), {
              headers: { 'Content-Type': 'application/json' },
          });
      }

      // --- Internal Endpoints for Actions ---

      // Internal endpoint to initialize DO state from schedule data
      if (url.pathname === '/internal/initialize-from-schedule' && request.method === 'POST') {
          try {
              const scheduleData = await request.json<MatchScheduleData>();
              // Basic validation for required fields in MatchScheduleData
               if (!scheduleData || scheduleData.tournamentMatchId === undefined || !scheduleData.round_name || scheduleData.team1_id === undefined || scheduleData.team2_id === undefined || !scheduleData.team1_name || !scheduleData.team2_name || !Array.isArray(scheduleData.team1_members) || !Array.isArray(scheduleData.team2_members) || !Array.isArray(scheduleData.team1_player_order_ids) || !Array.isArray(scheduleData.team2_player_order_ids) || !Array.isArray(scheduleData.match_song_list)) {
                   return new Response(JSON.stringify({ success: false, error: "Invalid schedule data payload" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
               }

              const initResult = await this.initializeFromSchedule(scheduleData);
              if (initResult.success) {
                  return new Response(JSON.stringify({ success: true, message: initResult.message }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  return new Response(JSON.stringify({ success: false, error: initResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing initialize-from-schedule payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid initialize-from-schedule payload', details: e.message }), { status: 400 });
          }
      }

      // For all other internal actions, matchData must be initialized
      if (!this.matchData || this.matchData.tournament_match_id === -1) {
           return new Response(JSON.stringify({ success: false, error: "Match is not initialized from schedule." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
      }


      // Internal endpoint to calculate round outcome
      if (url.pathname === '/internal/calculate-round' && request.method === 'POST') {
          try {
              const payload = await request.json<CalculateRoundPayload>();
              if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
                  return new Response(JSON.stringify({ success: false, error: "Invalid calculate-round payload: percentages must be numbers." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
              const result = await this.calculateRoundOutcome(payload);
              if (result.success) {
                  return new Response(JSON.stringify({ success: true, message: result.message, roundSummary: result.roundSummary }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  // Return 400 if it's a state-related error, 500 for internal calculation/save errors
                  const status = result.message?.includes("Cannot calculate round") ? 400 : 500;
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: status, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing calculate-round payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid calculate-round payload', details: e.message }), { status: 400 });
          }
      }


      // Internal endpoint to archive current round data to D1
      // Note: This is primarily called automatically by nextRound, but exposed for manual trigger if needed.
      if (url.pathname === '/internal/archive-round' && request.method === 'POST') {
          const archiveResult = await this.archiveCurrentRound();
          if (archiveResult.success) {
              return new Response(JSON.stringify({ success: true, message: archiveResult.message, d1RecordId: archiveResult.d1RecordId }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: archiveResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
          }
      }

      // Internal endpoint to advance to the next round
      if (url.pathname === '/internal/next-round' && request.method === 'POST') {
          const nextRoundResult = await this.nextRound();
          if (nextRoundResult.success) {
              return new Response(JSON.stringify({ success: true, message: nextRoundResult.message }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: nextRoundResult.message }), { status: 400, headers: { 'Content-Type': 'application/json' } }); // 400 for state issues
          }
      }

      // Internal endpoint to archive the entire match
      if (url.pathname === '/internal/archive-match' && request.method === 'POST') {
          const archiveResult = await this.archiveMatch();
          if (archiveResult.success) {
              return new Response(JSON.stringify({ success: true, message: archiveResult.message, d1RecordId: archiveResult.d1RecordId }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: archiveResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
          }
      }

      // Internal endpoint to resolve a draw
      if (url.pathname === '/internal/resolve-draw' && request.method === 'POST') {
          try {
              const payload = await request.json<ResolveDrawPayload>();
              if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
                  return new Response(JSON.stringify({ success: false, error: "Invalid resolve-draw payload: winner must be 'teamA' or 'teamB'." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
              const result = await this.resolveDraw(payload.winner);
              if (result.success) {
                  return new Response(JSON.stringify({ success: true, message: result.message }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: 400, headers: { 'Content-Type': 'application/json' } }); // 400 for state issues
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing resolve_draw payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid resolve-draw payload', details: e.message }), { status: 400 });
          }
      }

      // Internal endpoint for Staff to select a tiebreaker song
      if (url.pathname === '/internal/select-tiebreaker-song' && request.method === 'POST') {
           try {
               const payload = await request.json<SelectTiebreakerSongPayload>();
               if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string') {
                   return new Response(JSON.stringify({ success: false, error: "Invalid select-tiebreaker-song payload: song_id (number) and selected_difficulty (string) are required." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
               }
               const result = await this.selectTiebreakerSong(payload);
               if (result.success) {
                   return new Response(JSON.stringify({ success: true, message: result.message }), { headers: { 'Content-Type': 'application/json' } });
               } else {
                   return new Response(JSON.stringify({ success: false, error: result.message }), { status: 400, headers: { 'Content-Type': 'application/json' } }); // 400 for state/logic issues
               }
           } catch (e: any) {
               console.error(`DO (${this.match_do_id}) Exception processing select_tiebreaker_song payload:`, e);
               return new Response(JSON.stringify({ success: false, error: 'Invalid select-tiebreaker-song payload', details: e.message }), { status: 400 });
           }
      }


      // Fallback for unmatched internal paths
      return new Response('DO Not Found.', { status: 404 });
  }
}
