// src/durable-objects/matchDo.ts
import type {
  MatchState,
  Env,
  Member,
  MatchScheduleData,
  CalculateRoundPayload,
  ResolveDrawPayload,
  InternalProfession,
  MatchSong,
  RoundSummary,
  SelectTiebreakerSongPayload,
  Song, // Import Song type
  SongLevel // Import SongLevel type
} from '../types';

// Constants for game logic
const INITIAL_HEALTH = 100;
const MIRROR_HEALTH_RESTORE = 20;
const MAX_DAMAGE_DIGIT = 10; // 0% completion corresponds to 10 damage
const STANDARD_ROUNDS_COUNT = 6; // Number of standard rounds (BO6)

// Default state for a new match (fallback, initialization should come from schedule)
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
  match_do_id: string;
  websockets: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
      this.state = state;
      this.env = env;
      this.match_do_id = state.id.toString();

      this.state.blockConcurrencyWhile(async () => {
          const storedMatchData = await this.state.storage.get<MatchState>('matchData');
          if (storedMatchData) {
              this.matchData = storedMatchData;
              // Basic migration/initialization for new fields if loading old state
              if (this.matchData && this.matchData.match_song_list === undefined) {
                  console.warn(`DO (${this.match_do_id}): Initializing new fields for old state.`);
                  this.matchData.match_song_list = this.matchData.match_song_list ?? [];
                  this.matchData.current_song = this.matchData.current_song ?? null;
                  this.matchData.roundSummary = this.matchData.roundSummary ?? null;
                  this.matchData.round_name = (this.matchData as any).tournament_round ?? '未知轮次';
                  this.matchData.teamA_id = (this.matchData as any).teamA_id ?? -1;
                  this.matchData.teamB_id = (this.matchData as any).teamB_id ?? -1;
                  this.matchData.teamA_current_player_id = this.matchData.teamA_current_player_id ?? null;
                  this.matchData.teamB_current_player_id = this.matchData.teamB_current_player_id ?? null;
                  this.matchData.teamA_current_player_nickname = this.matchData.teamA_current_player_nickname ?? '未知选手';
                  this.matchData.teamB_current_player_nickname = this.matchData.teamB_current_player_nickname ?? '未知选手';
                  this.matchData.teamA_current_player_profession = this.matchData.teamA_current_player_profession ?? null;
                  this.matchData.teamB_current_player_profession = this.matchData.teamB_current_player_profession ?? null;

                  if ((this.matchData as any).status === 'pending') this.matchData.status = 'pending_scores';
                  if ((this.matchData as any).status === 'round_finished') this.matchData.status = 'round_finished';
                  if ((this.matchData as any).status === 'archived_in_d1') this.matchData.status = 'archived';

                  await this.state.storage.put('matchData', this.matchData);
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
              console.warn(`DO (${this.match_do_id}): Initializing with minimal state. Waiting for schedule initialization.`);
              this.matchData = {
                  match_do_id: this.match_do_id,
                  tournament_match_id: -1,
                  status: 'scheduled',
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
  private parseDamageDigits(percentage: number): number[] {
      const clampedPercentage = Math.max(0, Math.min(101.0000, percentage));
      const percentageString = clampedPercentage.toFixed(4);
      const parts = percentageString.split('.');

      if (parts.length !== 2) {
          console.error(`DO (${this.match_do_id}): Unexpected percentage format after toFixed: ${percentageString} (original: ${percentage})`);
          return [MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT];
      }

      const digitsString = parts[1];
      const digits: number[] = [];
      for (let i = 0; i < 4; i++) {
          const digitChar = digitsString[i] || '0';
          const digit = parseInt(digitChar, 10);
          digits.push(digit === 0 ? MAX_DAMAGE_DIGIT : digit);
      }
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
  private async initializeFromSchedule(scheduleData: MatchScheduleData): Promise<{ success: boolean; message?: string }> {
      console.log(`DO (${this.match_do_id}): Initializing from schedule for tournament match ${scheduleData.tournamentMatchId}`);

      if (this.matchData?.tournament_match_id !== scheduleData.tournamentMatchId || this.matchData?.status === 'archived') {
          await this.state.storage.deleteAll();
          console.log(`DO (${this.match_do_id}): Cleared storage for new initialization.`);
      } else {
          if (this.matchData?.status !== 'pending_scores') {
               console.warn(`DO (${this.match_do_id}): Match is in status ${this.matchData.status}. Re-initializing.`);
          } else {
               console.log(`DO (${this.match_do_id}): Match is already pending scores. Returning current state.`);
               this.broadcast(this.matchData);
               return { success: true, message: "Match already initialized and pending scores." };
          }
      }

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

      if (!Array.isArray(scheduleData.match_song_list) || scheduleData.match_song_list.length === 0) {
           const msg = "Invalid or empty match song list provided for initialization.";
           console.error(`DO (${this.match_do_id}): ${msg}`);
           return { success: false, message: msg };
      }
      // TODO: Add validation for individual MatchSong objects


      const { playerAId, playerBId } = this.getCurrentPlayers({
           ...defaultMatchState,
           match_do_id: this.match_do_id,
           tournament_match_id: scheduleData.tournamentMatchId,
           teamA_player_order_ids: scheduleData.team1_player_order_ids,
           teamB_player_order_ids: scheduleData.team2_player_order_ids,
           current_match_song_index: 0,
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


      this.matchData = {
          match_do_id: this.match_do_id,
          tournament_match_id: scheduleData.tournamentMatchId,
          round_name: scheduleData.round_name,
          current_match_song_index: 0,
          teamA_id: scheduleData.team1_id,
          teamB_id: scheduleData.team2_id,
          teamA_name: scheduleData.team1_name,
          teamB_name: scheduleData.team2_name,
          teamA_score: INITIAL_HEALTH,
          teamB_score: INITIAL_HEALTH,
          teamA_members: scheduleData.team1_members,
          teamB_members: scheduleData.team2_members,
          teamA_player_order_ids: scheduleData.team1_player_order_ids,
          teamB_player_order_ids: scheduleData.team2_player_order_ids,
          teamA_current_player_id: playerAId,
          teamB_current_player_id: playerBId,
          teamA_current_player_nickname: memberA?.nickname || '未知选手',
          teamB_current_player_nickname: memberB?.nickname || '未知选手',
          teamA_current_player_profession: this.getInternalProfession(memberA?.job),
          teamB_current_player_profession: this.getInternalProfession(memberB?.job),
          teamA_mirror_available: true,
          teamB_mirror_available: true,
          match_song_list: scheduleData.match_song_list,
          current_song: scheduleData.match_song_list[0] || null,
          roundSummary: null,
          status: 'pending_scores',
      };

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
      if (this.matchData.status !== 'pending_scores') {
          const msg = `Match is not in 'pending_scores' status (${this.matchData.status}). Cannot calculate round.`;
          console.warn(`DO (${this.match_do_id}): ${msg}`);
          return { success: false, message: msg };
      }
      if (!this.matchData.current_song) {
           const msg = "No current song set for calculation.";
           console.error(`DO (${this.match_do_id}): ${msg}`);
           return { success: false, message: msg };
      }


      console.log(`DO (${this.match_do_id}) Calculating Round ${this.matchData.current_match_song_index + 1} with A: ${payload.teamA_percentage}%, B: ${payload.teamB_percentage}%`);

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

      let teamAModifiedDamageToB = teamABaseDamage;
      let teamBModifiedDamageToA = teamBBaseDamage;
      let teamAHealFromSupporterSkill = 0;
      let teamBHealFromSupporterSkill = 0;
      let teamAProfessionEffectLog = '';
      let teamBProfessionEffectLog = '';


      if (teamACurrentProfession === 'attacker') {
          teamAModifiedDamageToB += teamAMaxDigitDamage;
          teamAProfessionEffectLog = `绝剑士技能：追加最高位数字伤害 ${teamAMaxDigitDamage}。`;
      }
      if (teamBCurrentProfession === 'attacker') {
          teamBModifiedDamageToA += teamBMaxDigitDamage;
          teamBProfessionEffectLog = `绝剑士技能：追加最高位数字伤害 ${teamBMaxDigitDamage}。`;
      }

      if (teamACurrentProfession === 'defender' && teamBDamageDigits.length > 0) {
          const randomIndex = Math.floor(Math.random() * teamBDamageDigits.length);
          const invalidatedDamage = teamBDamageDigits[randomIndex];
          teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - invalidatedDamage);
          teamAProfessionEffectLog = `矩盾手技能：无效化对方随机一位数字伤害 ${invalidatedDamage}。`;
      }
      if (teamBCurrentProfession === 'defender' && teamADamageDigits.length > 0) {
          const randomIndex = Math.floor(Math.random() * teamADamageDigits.length);
          const invalidatedDamage = teamADamageDigits[randomIndex];
          teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - invalidatedDamage);
          teamBProfessionEffectLog = `矩盾手技能：无效化对方随机一位数字伤害 ${invalidatedDamage}。`;
      }

      if (teamACurrentProfession === 'supporter' && teamADamageDigits.length >= 2) {
          const sortedDigits = [...teamADamageDigits].sort((a, b) => a - b);
          const lowest = sortedDigits[0];
          const highest = sortedDigits[sortedDigits.length - 1];
          const conversion = lowest + highest;
          teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - conversion);
          teamAHealFromSupporterSkill += conversion;
          teamAProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
      }
      if (teamBCurrentProfession === 'supporter' && teamBDamageDigits.length >= 2) {
          const sortedDigits = [...teamBDamageDigits].sort((a, b) => a - b);
          const lowest = sortedDigits[0];
          const highest = sortedDigits[sortedDigits.length - 1];
          const conversion = lowest + highest;
          teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - conversion);
          teamBHealFromSupporterSkill += conversion;
          teamBProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
      }

      let currentAHealth = this.matchData.teamA_score;
      let currentBHealth = this.matchData.teamB_score;

      let healthAfterDamageA = currentAHealth - teamBModifiedDamageToA;
      let healthAfterDamageB = currentBHealth - teamAModifiedDamageToB;

      let rawOverflowDamageToA = healthAfterDamageA < 0 ? Math.abs(healthAfterDamageA) : 0;
      let rawOverflowDamageToB = healthAfterDamageB < 0 ? Math.abs(healthAfterDamageB) : 0;

      let teamAMirrorUsedThisTurn = false;
      let teamBMirrorUsedThisTurn = false;
      let teamAHealFromSupporterMirrorBonus = 0;
      let teamBHealFromSupporterMirrorBonus = 0;
      let teamAReflectedDamageByDefenderMirror = 0;
      let teamBReflectedDamageByDefenderMirror = 0;
      let teamAAttackerMirrorExtraDamage = 0;
      let teamBAttackerMirrorExtraDamage = 0;
      let teamAMirrorEffectLog = '';
      let teamBMirrorEffectLog = '';

      let finalHealthA = healthAfterDamageA;
      let finalHealthB = healthAfterDamageB;

      const canAInitiallyTriggerMirror = finalHealthA <= 0 && this.matchData.teamA_mirror_available;
      const canBInitiallyTriggerMirror = finalHealthB <= 0 && this.matchData.teamB_mirror_available;

      if (canAInitiallyTriggerMirror && canBInitiallyTriggerMirror) {
          this.matchData.teamA_mirror_available = false;
          this.matchData.teamB_mirror_available = false;
          teamAMirrorUsedThisTurn = true;
          teamBMirrorUsedThisTurn = true;
          finalHealthA = MIRROR_HEALTH_RESTORE;
          finalHealthB = MIRROR_HEALTH_RESTORE;
          teamAMirrorEffectLog = '双方同时触发复影折镜，血量恢复至20。';
          teamBMirrorEffectLog = '双方同时触发复影折镜，血量恢复至20。';
      } else {
          if (canAInitiallyTriggerMirror) {
              this.matchData.teamA_mirror_available = false;
              teamAMirrorUsedThisTurn = true;
              finalHealthA = MIRROR_HEALTH_RESTORE;
              teamAMirrorEffectLog = '触发复影折镜，血量恢复至20。';

              if (teamACurrentProfession === 'attacker') {
                  teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage;
                  finalHealthB -= teamAAttackerMirrorExtraDamage;
                  teamAMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamAAttackerMirrorExtraDamage}。`;
              } else if (teamACurrentProfession === 'defender') {
                  teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA;
                  finalHealthB -= teamAReflectedDamageByDefenderMirror;
                  teamAMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamAReflectedDamageByDefenderMirror}。`;
              } else if (teamACurrentProfession === 'supporter') {
                  teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill;
                  teamAMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamAHealFromSupporterMirrorBonus}。`;
              }
          }

          const canBTriggerAfterAPass1 = finalHealthB <= 0 && this.matchData.teamB_mirror_available && !teamBMirrorUsedThisTurn;
          if (canBTriggerAfterAPass1) {
              this.matchData.teamB_mirror_available = false;
              teamBMirrorUsedThisTurn = true;
              finalHealthB = MIRROR_HEALTH_RESTORE;
              teamBMirrorEffectLog = '触发复影折镜，血量恢复至20。';

              if (teamBCurrentProfession === 'attacker') {
                  teamBAttackerMirrorExtraDamage = teamBMaxDigitDamage;
                  finalHealthA -= teamBAttackerMirrorExtraDamage;
                  teamBMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamBAttackerMirrorExtraDamage}。`;
              } else if (teamBCurrentProfession === 'defender') {
                  teamBReflectedDamageByDefenderMirror = rawOverflowDamageToB;
                  finalHealthA -= teamBReflectedDamageByDefenderMirror;
                  teamBMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamBReflectedDamageByDefenderMirror}。`;
              } else if (teamBCurrentProfession === 'supporter') {
                  teamBHealFromSupporterMirrorBonus = teamBHealFromSupporterSkill;
                  teamBMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamBHealFromSupporterMirrorBonus}。`;
              }
          }

          const canATriggerAfterBPass2 = finalHealthA <= 0 && this.matchData.teamA_mirror_available && !teamAMirrorUsedThisTurn;
          if (canATriggerAfterBPass2) {
              this.matchData.teamA_mirror_available = false;
              teamAMirrorUsedThisTurn = true;
              finalHealthA = MIRROR_HEALTH_RESTORE;
              teamAMirrorEffectLog = '触发复影折镜 (连锁反应)，血量恢复至20。';

              if (teamACurrentProfession === 'attacker') {
                  teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage;
                  finalHealthB -= teamAAttackerMirrorExtraDamage;
                  teamAMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamAAttackerMirrorExtraDamage}。`;
              } else if (teamACurrentProfession === 'defender') {
                  teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA;
                  finalHealthB -= teamAReflectedDamageByDefenderMirror;
                  teamAMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamAReflectedDamageByDefenderMirror}。`;
              } else if (teamACurrentProfession === 'supporter') {
                  teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill;
                  teamAMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamAHealFromSupporterMirrorBonus}。`;
              }
          }
      }

      finalHealthA += (teamAHealFromSupporterSkill + teamAHealFromSupporterMirrorBonus);
      finalHealthB += (teamBHealFromSupporterSkill + teamBHealFromSupporterMirrorBonus);

      finalHealthA += teamAEffectValue;
      finalHealthB += teamBEffectValue;

      const healthBeforeRoundingA = finalHealthA;
      const healthBeforeRoundingB = finalHealthB;

      this.matchData.teamA_score = Math.round(finalHealthA);
      this.matchData.teamB_score = Math.round(finalHealthB);

      const aDead = this.matchData.teamA_score <= 0;
      const bDead = this.matchData.teamB_score <= 0;

      let newStatus: MatchState['status'];
      let matchEnded = false;

      if (aDead && bDead) {
          if (this.matchData.teamA_score > this.matchData.teamB_score) {
              newStatus = 'team_A_wins';
          } else if (this.matchData.teamB_score > this.matchData.teamA_score) {
              newStatus = 'team_B_wins';
          } else {
              newStatus = 'draw_pending_resolution';
          }
          matchEnded = true;
      } else if (aDead) {
          newStatus = 'team_B_wins';
          matchEnded = true;
      } else if (bDead) {
          newStatus = 'team_A_wins';
          matchEnded = true;
      } else {
          if (this.matchData.current_match_song_index >= STANDARD_ROUNDS_COUNT - 1 && !(this.matchData.current_song?.is_tiebreaker_song)) {
               // Finished standard rounds (index 5 is the 6th song), and current song is NOT a tiebreaker
               newStatus = 'tiebreaker_pending_song';
          } else {
               newStatus = 'round_finished';
          }
      }
      this.matchData.status = newStatus;

      if (this.matchData.current_song) {
           this.matchData.current_song.status = 'completed';
           this.matchData.current_song.teamA_player_id = this.matchData.teamA_current_player_id ?? undefined;
           this.matchData.current_song.teamB_player_id = this.matchData.teamB_current_player_id ?? undefined;
           this.matchData.current_song.teamA_percentage = teamAPercentage;
           this.matchData.current_song.teamB_percentage = teamBPercentage;
           this.matchData.current_song.teamA_damage_dealt = currentBHealth - finalHealthB;
           this.matchData.current_song.teamB_damage_dealt = currentAHealth - finalHealthA;
           this.matchData.current_song.teamA_effect_value = teamAEffectValue;
           this.matchData.current_song.teamB_effect_value = teamBEffectValue;
           this.matchData.current_song.teamA_health_after = this.matchData.teamA_score;
           this.matchData.current_song.teamB_health_after = this.matchData.teamB_score;
           this.matchData.current_song.teamA_mirror_triggered = teamAMirrorUsedThisTurn;
           this.matchData.current_song.teamB_mirror_triggered = teamBMirrorUsedThisTurn;
      }


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
          teamBMirrorEffectLog: teamBMirrorEffectLog, // Fix typo here
          teamA_supporter_base_skill_heal: teamAHealFromSupporterSkill,
          teamB_supporter_base_skill_heal: teamBHealFromSupporterSkill,
          teamA_supporter_mirror_bonus_heal: teamAHealFromSupporterMirrorBonus,
          teamB_supporter_mirror_bonus_heal: teamBHealFromSupporterMirrorBonus,

          teamA_final_damage_dealt: currentBHealth - finalHealthB,
          teamB_final_damage_dealt: currentAHealth - finalHealthA,

          teamA_health_change: this.matchData.teamA_score - currentAHealth,
          teamB_health_change: this.matchData.teamB_score - currentBHealth,
          teamA_health_after: this.matchData.teamA_score,
          teamB_health_after: this.matchData.teamB_score,

          is_tiebreaker_song: this.matchData.current_song?.is_tiebreaker_song ?? false,

          log: [],
      };
      this.matchData.roundSummary = summary;


      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);

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
              summary.round_number_in_match,
              summary.song_id,
              summary.selected_difficulty,
              currentSong.picker_team_id,
              currentSong.picker_member_id,
              summary.teamA_player_id,
              summary.teamB_player_id,
              summary.teamA_percentage,
              summary.teamB_percentage,
              summary.teamA_final_damage_dealt,
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
      if (this.matchData.status !== 'round_finished') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'round_finished' to advance.` };
      }

      const archiveResult = await this.archiveCurrentRound();
      if (!archiveResult.success) {
          console.warn(`DO (${this.match_do_id}) Failed to auto-archive current round ${this.matchData.current_match_song_index + 1} before advancing:`, archiveResult.message);
      }

      const nextSongIndex = this.matchData.current_match_song_index + 1;

      if (nextSongIndex >= this.matchData.match_song_list.length) {
           const msg = `Cannot advance round. No song found at index ${nextSongIndex}. Match song list length: ${this.matchData.match_song_list.length}.`;
           console.error(`DO (${this.match_do_id}): ${msg}`);
           if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'tiebreaker_pending_song', 'archived'].includes(this.matchData.status)) {
                this.matchData.status = 'completed';
                await this.state.storage.put('matchData', this.matchData);
                this.broadcast(this.matchData);
           }
           return { success: false, message: msg };
      }

      this.matchData.current_match_song_index = nextSongIndex;
      this.matchData.current_song = this.matchData.match_song_list[nextSongIndex];
      this.matchData.current_song.status = 'ongoing';

      const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
      const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
      const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

      this.matchData.teamA_current_player_id = playerAId;
      this.matchData.teamB_current_player_id = playerBId;
      this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
      this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
      this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
      this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);


      this.matchData.roundSummary = null;
      this.matchData.status = 'pending_scores';

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
  // This method now receives song_details from the Worker
  private async selectTiebreakerSong(payload: SelectTiebreakerSongPayload & { song_details: Song }): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to select tiebreaker song." };
      }
      if (this.matchData.status !== 'tiebreaker_pending_song') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'tiebreaker_pending_song' to select tiebreaker.` };
      }

      console.log(`DO (${this.match_do_id}) Staff selecting tiebreaker song: ${payload.song_id} (${payload.selected_difficulty})`);

      try {
          const song = payload.song_details; // Use song details passed from Worker
          const parsedLevels: SongLevel = song.levels_json ? JSON.parse(song.levels_json) : {};
          const difficultyValue = parsedLevels[payload.selected_difficulty as keyof SongLevel] || '??';
          const fullDifficultyString = `${payload.selected_difficulty} ${difficultyValue}`;

          const tiebreakerSong: MatchSong = {
              song_id: song.id,
              song_title: song.title,
              song_difficulty: fullDifficultyString,
              song_element: song.category === 'original' ? 'fire' : song.category === 'niconico' ? 'wood' : null, // Example mapping
              cover_filename: song.cover_filename,
              bpm: song.bpm,
              fullCoverUrl: song.cover_filename ? `https://${this.env.SONG_COVER_BUCKET.name}.r2.dev/${song.cover_filename}` : undefined, // Assuming SONG_COVER_BUCKET binding

              // Assign picker info (can use a special ID for Staff/System)
              // TODO: Define a Staff/System member ID or team ID for tiebreakers
              picker_member_id: -1, // Placeholder for Staff/System
              picker_team_id: -1, // Placeholder for Staff/System
              is_tiebreaker_song: true,

              status: 'pending',
          };

          this.matchData.match_song_list.push(tiebreakerSong);

          this.matchData.current_match_song_index = this.matchData.match_song_list.length - 1;
          this.matchData.current_song = tiebreakerSong;
          this.matchData.current_song.status = 'ongoing';

          // Determine players for the tiebreaker round
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


          this.matchData.roundSummary = null;
          this.matchData.status = 'pending_scores';

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
      if (this.matchData.status === 'archived') {
          return { success: true, message: "Match already archived.", d1RecordId: this.match_do_id };
      }
      if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'completed'].includes(this.matchData.status)) {
           console.warn(`DO (${this.match_do_id}): Archiving match from non-final state: ${this.matchData.status}`);
      }


      try {
          // Ensure the last round's summary is archived
          if (this.matchData.roundSummary && this.matchData.roundSummary.round_number_in_match === this.matchData.current_match_song_index + 1) {
               await this.archiveCurrentRound();
          }
          // Note: This assumes archiveCurrentRound was called after *every* completed round.
          // If not, you'd need logic here to iterate through match_song_list and archive any completed songs
          // that haven't been archived yet.


          if (this.matchData.tournament_match_id && this.matchData.tournament_match_id !== -1) {
              try {
                  const winnerTeamId = this.determineWinnerTeamId(this.matchData);

                  const tournamentMatchStatus = ['team_A_wins', 'team_B_wins'].includes(this.matchData.status) ? 'completed'
                                                  : this.matchData.status === 'draw_pending_resolution' ? 'completed'
                                                  : 'archived';

                  const updateTournamentStmt = this.env.DB.prepare(
                      `UPDATE tournament_matches SET
                         status = ?,
                         winner_team_id = ?,
                         final_score_team1 = ?,
                         final_score_team2 = ?,
                         match_do_id = ?,
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
                  } else {
                      console.log(`DO (${this.match_do_id}) updated tournament_matches entry ${this.matchData.tournament_match_id} status to '${tournamentMatchStatus}'.`);
                  }

              } catch (e: any) {
                  console.error(`DO (${this.match_do_id}) exception during tournament_matches update:`, e);
              }
          }


          this.matchData.status = 'archived';
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);

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

      if (winnerDesignation === 'teamA') {
          this.matchData.status = 'team_A_wins';
          if (this.matchData.teamB_score > 0) this.matchData.teamB_score = 0;
      } else if (winnerDesignation === 'teamB') {
          this.matchData.status = 'team_B_wins';
          if (this.matchData.teamA_score > 0) this.matchData.teamA_score = 0;
      } else {
          return { success: false, message: "Invalid winner designation." };
      }

      const archiveResult = await this.archiveMatch();
      if (!archiveResult.success) {
          console.error(`DO (${this.match_do_id}) Failed to auto-archive match after draw resolution:`, archiveResult.message);
          return { success: false, message: `Draw resolved, but failed to archive match: ${archiveResult.message}` };
      }

      console.log(`DO (${this.match_do_id}) Draw resolved. New status: ${this.matchData.status}`);
      return { success: true, message: `Draw resolved. ${this.matchData.status.replace('_', ' ')}.` };
  }


  async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);

      if (url.pathname === '/websocket') {
          if (request.headers.get('Upgrade') !== 'websocket') {
              return new Response('Expected Upgrade: websocket', { status: 426 });
          }
          const [client, server] = Object.values(new WebSocketPair());
          this.websockets.push(server);
          server.accept();
          console.log(`DO (${this.match_do_id}) WebSocket connected. Total: ${this.websockets.length}`);

          if (this.matchData) {
               server.send(JSON.stringify(this.matchData));
          } else {
               server.send(JSON.stringify({ success: false, error: "Match data not initialized in DO" }));
          }

          server.addEventListener('message', event => {
              console.log(`DO (${this.match_do_id}) WS message from client:`, event.data);
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

      if (url.pathname === '/state' && request.method === 'GET') {
           if (!this.matchData) {
               return new Response(JSON.stringify({ success: false, error: "Match data not initialized in DO" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
           }
          return new Response(JSON.stringify(this.matchData), {
              headers: { 'Content-Type': 'application/json' },
          });
      }

      // --- Internal Endpoints for Actions ---

      if (url.pathname === '/internal/initialize-from-schedule' && request.method === 'POST') {
          try {
              const scheduleData = await request.json<MatchScheduleData>();
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

      if (!this.matchData || this.matchData.tournament_match_id === -1) {
           return new Response(JSON.stringify({ success: false, error: "Match is not initialized from schedule." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
      }


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
                  const status = result.message?.includes("Cannot calculate round") ? 400 : 500;
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: status, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing calculate-round payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid calculate-round payload', details: e.message }), { status: 400 });
          }
      }


      if (url.pathname === '/internal/archive-round' && request.method === 'POST') {
          const archiveResult = await this.archiveCurrentRound();
          if (archiveResult.success) {
              return new Response(JSON.stringify({ success: true, message: archiveResult.message, d1RecordId: archiveResult.d1RecordId }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: archiveResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
          }
      }

      if (url.pathname === '/internal/next-round' && request.method === 'POST') {
          const nextRoundResult = await this.nextRound();
          if (nextRoundResult.success) {
              return new Response(JSON.stringify({ success: true, message: nextRoundResult.message }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: nextRoundResult.message }), { status: 400, headers: { 'Content-Type': 'application/json' } });
          }
      }

      if (url.pathname === '/internal/archive-match' && request.method === 'POST') {
          const archiveResult = await this.archiveMatch();
          if (archiveResult.success) {
              return new Response(JSON.stringify({ success: true, message: archiveResult.message, d1RecordId: archiveResult.d1RecordId }), { headers: { 'Content-Type': 'application/json' } });
          } else {
              return new Response(JSON.stringify({ success: false, error: archiveResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
          }
      }

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
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing resolve_draw payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid resolve-draw payload', details: e.message }), { status: 400 });
          }
      }

      // Internal endpoint for Staff to select a tiebreaker song
      // This now expects the payload to include song_details
      if (url.pathname === '/internal/select-tiebreaker-song' && request.method === 'POST') {
           try {
               // The payload now includes song_details fetched by the Worker
               interface InternalSelectTiebreakerPayload extends SelectTiebreakerSongPayload {
                   song_details: Song;
               }
               const payload = await request.json<InternalSelectTiebreakerPayload>();

               if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string' || !payload.song_details || typeof payload.song_details.id !== 'number') {
                   return new Response(JSON.stringify({ success: false, error: "Invalid select-tiebreaker-song payload: song_id (number), selected_difficulty (string), and song_details (Song) are required." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
               }
               // Basic check that song_id matches song_details.id
               if (payload.song_id !== payload.song_details.id) {
                    console.warn(`DO (${this.match_do_id}): select_tiebreaker_song payload song_id (${payload.song_id}) does not match song_details.id (${payload.song_details.id}). Using song_details.`);
                    // Decide how to handle this discrepancy - using song_details is safer
               }


               const result = await this.selectTiebreakerSong(payload); // Pass the payload including song_details
               if (result.success) {
                   return new Response(JSON.stringify({ success: true, message: result.message }), { headers: { 'Content-Type': 'application/json' } });
               } else {
                   return new Response(JSON.stringify({ success: false, error: result.message }), { status: 400, headers: { 'Content-Type': 'application/json' } });
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
