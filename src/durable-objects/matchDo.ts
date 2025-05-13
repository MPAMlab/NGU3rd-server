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
  SongLevel, // Import SongLevel type
  ApiResponse // Import ApiResponse if needed for internal responses, though DOs usually return Response directly
} from '../types'; // Adjust path to your types file

// Constants for game logic
const INITIAL_HEALTH = 100;
const MIRROR_HEALTH_RESTORE = 20;
const MAX_DAMAGE_DIGIT = 10; // 0% completion corresponds to 10 damage
const STANDARD_ROUNDS_COUNT = 6; // Number of standard rounds (BO6)

// Default state for a new match (fallback, initialization should come from schedule)
// This is used only if no state is found in storage AND no initialization request is received.
// The initializeFromSchedule method is the primary way to set up a match.
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
      // The DO ID is a 64-hex-digit string derived by the Worker using idFromName
      this.match_do_id = state.id.toString();

      // Block concurrent requests while loading state from storage
      this.state.blockConcurrencyWhile(async () => {
          console.log(`DO (${this.match_do_id}): Loading state from storage.`);
          const storedMatchData = await this.state.storage.get<MatchState>('matchData');

          if (storedMatchData) {
              this.matchData = storedMatchData;
              console.log(`DO (${this.match_do_id}): State loaded. Status: ${this.matchData.status}`);

              // --- State Migration/Initialization for new fields if loading old state ---
              // This block helps ensure compatibility if you add new fields to MatchState
              if (this.matchData && this.matchData.match_song_list === undefined) {
                  console.warn(`DO (${this.match_do_id}): Initializing new fields for old state.`);
                  this.matchData.match_song_list = this.matchData.match_song_list ?? [];
                  this.matchData.current_song = this.matchData.current_song ?? null;
                  this.matchData.roundSummary = this.matchData.roundSummary ?? null;
                  // Example: Migrate old field name 'tournament_round' to 'round_name'
                  this.matchData.round_name = (this.matchData as any).tournament_round ?? '未知轮次';
                  this.matchData.teamA_id = (this.matchData as any).teamA_id ?? -1;
                  this.matchData.teamB_id = (this.matchData as any).teamB_id ?? -1;
                  this.matchData.teamA_current_player_id = this.matchData.teamA_current_player_id ?? null;
                  this.matchData.teamB_current_player_id = this.matchData.teamB_current_player_id ?? null;
                  this.matchData.teamA_current_player_nickname = this.matchData.teamA_current_player_nickname ?? '未知选手';
                  this.matchData.teamB_current_player_nickname = this.matchData.teamB_current_player_nickname ?? '未知选手';
                  this.matchData.teamA_current_player_profession = this.matchData.teamA_current_player_profession ?? null;
                  this.matchData.teamB_current_player_profession = this.matchData.teamB_current_player_profession ?? null;

                  // Example: Migrate old status values
                  if ((this.matchData as any).status === 'pending') this.matchData.status = 'pending_scores';
                  if ((this.matchData as any).status === 'round_finished') this.matchData.status = 'round_finished';
                  if ((this.matchData as any).status === 'archived_in_d1') this.matchData.status = 'archived';

                  // Save the migrated state
                  await this.state.storage.put('matchData', this.matchData);
              }
              // --- End State Migration ---


               // Ensure current_song is correctly set based on index and list on load
               if (this.matchData && this.matchData.match_song_list && this.matchData.match_song_list.length > this.matchData.current_match_song_index) {
                   this.matchData.current_song = this.matchData.match_song_list[this.matchData.current_match_song_index];
                   // Ensure fullCoverUrl is present on the current song if cover_filename exists
                   if (this.matchData.current_song.cover_filename && this.env.SONG_COVER_BUCKET?.name) {
                       this.matchData.current_song.fullCoverUrl = `https://${this.env.SONG_COVER_BUCKET.name}.r2.dev/${this.matchData.current_song.cover_filename}`;
                   } else {
                       this.matchData.current_song.fullCoverUrl = undefined;
                   }
               } else {
                   this.matchData.current_song = null;
               }

               // Ensure current players are set based on index and order on load
               const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
               const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
               const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

               this.matchData.teamA_current_player_id = playerAId;
               this.matchData.teamB_current_player_id = playerBId;
               this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
               this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
               this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
               this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);


          } else {
              console.warn(`DO (${this.match_do_id}): No state found in storage. Initializing with minimal default state. Waiting for schedule initialization.`);
              // Initialize with minimal state, expecting /internal/initialize-from-schedule soon
              this.matchData = {
                  match_do_id: this.match_do_id,
                  tournament_match_id: -1, // Indicate not yet linked to a D1 match
                  status: 'scheduled', // Start as scheduled, waiting for setup/start
                  round_name: '未知轮次',
                  current_match_song_index: 0,
                  teamA_id: -1, teamB_id: -1, // Indicate teams not yet set
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
                  teamA_members: [], // Initialize member lists
                  teamB_members: [],
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
  // 0% -> 10, 100% -> 0, 101%+ -> 0
  private parseDamageDigits(percentage: number): number[] {
      // Clamp percentage between 0 and 101 for consistent handling
      const clampedPercentage = Math.max(0, Math.min(101.0000, percentage));
      // Convert to string with 4 decimal places
      const percentageString = clampedPercentage.toFixed(4);
      const parts = percentageString.split('.');

      if (parts.length !== 2) {
          console.error(`DO (${this.match_do_id}): Unexpected percentage format after toFixed: ${percentageString} (original: ${percentage})`);
          // Fallback to max damage digits if parsing fails
          return [MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT];
      }

      const digitsString = parts[1];
      const digits: number[] = [];
      for (let i = 0; i < 4; i++) {
          const digitChar = digitsString[i] || '0'; // Use '0' if string is shorter than 4
          const digit = parseInt(digitChar, 10);
          // Convert 0 digit to MAX_DAMAGE_DIGIT (10)
          digits.push(digit === 0 ? MAX_DAMAGE_DIGIT : digit);
      }
      return digits;
  }


  // Broadcast the current match state to all connected WebSockets
  private broadcast(message: object | string) {
      const payload = typeof message === 'string' ? message : JSON.stringify(message);
      // Filter out closed connections before broadcasting
      this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
      this.websockets.forEach((ws) => {
          try {
              ws.send(payload);
          } catch (e) {
              console.error(`DO (${this.match_do_id}) Error sending message to WebSocket:`, e);
              // Consider removing the socket if sending fails
              // this.websockets = this.websockets.filter(w => w !== ws);
          }
      });
  }

  // Determine the winner team ID based on final scores
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
      // Use modulo to cycle through the player order list
      const playerAId = state.teamA_player_order_ids && state.teamA_player_order_ids.length > 0
          ? state.teamA_player_order_ids[state.current_match_song_index % state.teamA_player_order_ids.length]
          : null;
      const playerBId = state.teamB_player_order_ids && state.teamB_player_order_ids.length > 0
          ? state.teamB_player_order_ids[state.current_match_song_index % state.teamB_player_order_ids.length]
          : null;
      return { playerAId, playerBId };
  }


  // --- Internal Method: Initialize from Schedule ---
  // Called by the Worker when a match is started live.
  private async initializeFromSchedule(scheduleData: MatchScheduleData): Promise<{ success: boolean; message?: string }> {
      console.log(`DO (${this.match_do_id}): Initializing from schedule for tournament match ${scheduleData.tournamentMatchId}`);

      // If the DO is already initialized for this match and not archived,
      // assume it's a re-initialization request (e.g., Worker restarted)
      // and just broadcast the current state.
      if (this.matchData?.tournament_match_id === scheduleData.tournamentMatchId && this.matchData?.status !== 'archived') {
          console.log(`DO (${this.match_do_id}): Match ${scheduleData.tournamentMatchId} already initialized. Broadcasting current state.`);
          this.broadcast(this.matchData);
          return { success: true, message: "Match already initialized." };
      }

      // If initializing a new match or re-initializing an archived one, clear storage
      await this.state.storage.deleteAll();
      console.log(`DO (${this.match_do_id}): Cleared storage for new initialization.`);


      // Validation helpers
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

      const validateSongList = (songList: MatchSong[]) => {
           if (!Array.isArray(songList) || songList.length === 0) {
                console.error(`DO (${this.match_do_id}): Invalid or empty match song list.`);
                return false;
           }
           // TODO: Add more detailed validation for each song object if necessary
           return true;
      }


      // Perform validation
      if (!validateOrder(scheduleData.team1_player_order_ids, scheduleData.team1_members, scheduleData.team1_name) ||
          !validateOrder(scheduleData.team2_player_order_ids, scheduleData.team2_members, scheduleData.team2_name) ||
          !validateSongList(scheduleData.match_song_list)) {
          const msg = "Invalid initialization data provided (player order, members, or song list).";
          console.error(`DO (${this.match_do_id}): ${msg}`);
          // Set status to archived or error state if initialization fails critically
          this.matchData = { ...this.matchData, status: 'archived' } as MatchState; // Use type assertion
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);
          return { success: false, message: msg };
      }

      // Add fullCoverUrl to all songs in the list if cover_filename exists
      const processedMatchSongList = scheduleData.match_song_list.map(song => {
          const fullCoverUrl = song.cover_filename && this.env.SONG_COVER_BUCKET?.name
              ? `https://${this.env.SONG_COVER_BUCKET.name}.r2.dev/${song.cover_filename}`
              : undefined;
          return { ...song, fullCoverUrl };
      });


      // Determine initial players
      const { playerAId, playerBId } = this.getCurrentPlayers({
           // Pass necessary data structure for getCurrentPlayers helper
           teamA_player_order_ids: scheduleData.team1_player_order_ids,
           teamB_player_order_ids: scheduleData.team2_player_order_ids,
           current_match_song_index: 0, // Start at index 0
       } as MatchState); // Use type assertion as we only need a subset of MatchState


      const memberA = this.getMemberById(playerAId, scheduleData.team1_members);
      const memberB = this.getMemberById(playerBId, scheduleData.team2_members);


      // Construct the initial match state
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
          teamA_members: scheduleData.team1_members, // Store member lists for easy lookup
          teamB_members: scheduleData.team2_members,
          teamA_player_order_ids: scheduleData.team1_player_order_ids,
          teamB_player_order_ids: scheduleData.team2_player_order_ids,
          teamA_current_player_id: playerAId,
          teamB_current_player_id: playerBId,
          teamA_current_player_nickname: memberA?.nickname || '未知选手',
          teamB_current_player_nickname: memberB?.nickname || '未知选手',
          teamA_current_player_profession: this.getInternalProfession(memberA?.job),
          teamB_current_player_profession: this.getInternalProfession(memberB?.job),
          teamA_mirror_available: true, // Mirrors start available
          teamB_mirror_available: true,
          match_song_list: processedMatchSongList, // Use the processed list with fullCoverUrl
          current_song: processedMatchSongList[0] || null, // Set the first song as current
          roundSummary: null, // No summary yet
          status: 'pending_scores', // Match starts waiting for scores
      };

      // Set the status of the first song to ongoing
      if (this.matchData.current_song) {
           this.matchData.current_song.status = 'ongoing';
      }


      try {
          // Save the initial state to storage
          await this.state.storage.put('matchData', this.matchData);
          // Broadcast the initial state to any connected clients
          this.broadcast(this.matchData);
          console.log(`DO (${this.match_do_id}): State initialized from schedule and saved.`);
          return { success: true, message: "Match initialized from schedule." };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}): Failed to save initial state from schedule:`, e);
          // If saving fails, set status to archived to prevent further actions
          this.matchData.status = 'archived';
          this.broadcast(this.matchData);
          return { success: false, message: `Failed to initialize match: ${e.message}` };
      }
  }

  // --- Core Game Logic: Calculate Round Outcome ---
  // Called by the Worker after receiving scores from the frontend.
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

      // Ensure percentages and effect values are numbers, default to 0 if not
      const teamAPercentage = typeof payload.teamA_percentage === 'number' ? payload.teamA_percentage : 0;
      const teamBPercentage = typeof payload.teamB_percentage === 'number' ? payload.teamB_percentage : 0;
      const teamAEffectValue = typeof payload.teamA_effect_value === 'number' ? payload.teamA_effect_value : 0;
      const teamBEffectValue = typeof payload.teamB_effect_value === 'number' ? payload.teamB_effect_value : 0;


      // Calculate base damage from percentage digits
      const teamADamageDigits = this.parseDamageDigits(teamAPercentage);
      const teamBDamageDigits = this.parseDamageDigits(teamBPercentage);
      let teamABaseDamage = teamADamageDigits.reduce((sum, digit) => sum + digit, 0);
      let teamBBaseDamage = teamBDamageDigits.reduce((sum, digit) => sum + digit, 0);

      // Get current players' professions
      const teamACurrentProfession = this.matchData.teamA_current_player_profession;
      const teamBCurrentProfession = this.matchData.teamB_current_player_profession;
      let teamAMaxDigitDamage = Math.max(0, ...teamADamageDigits); // Max digit for Attacker skill
      let teamBMaxDigitDamage = Math.max(0, ...teamBDamageDigits);

      let teamAModifiedDamageToB = teamABaseDamage; // Damage A deals to B, modified by A's profession
      let teamBModifiedDamageToA = teamBBaseDamage; // Damage B deals to A, modified by B's profession
      let teamAHealFromSupporterSkill = 0; // Healing from Supporter's base skill
      let teamBHealFromSupporterSkill = 0;
      let teamAProfessionEffectLog = ''; // Log string for profession effects
      let teamBProfessionEffectLog = '';


      // Apply Profession Effects
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
          teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - conversion); // Supporter converts own damage to heal
          teamAHealFromSupporterSkill += conversion;
          teamAProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
      }
      if (teamBCurrentProfession === 'supporter' && teamBDamageDigits.length >= 2) {
          const sortedDigits = [...teamBDamageDigits].sort((a, b) => a - b);
          const lowest = sortedDigits[0];
          const highest = sortedDigits[sortedDigits.length - 1];
          const conversion = lowest + highest;
          teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - conversion); // Supporter converts own damage to heal
          teamBHealFromSupporterSkill += conversion;
          teamBProfessionEffectLog = `炼星师技能：转化最低位(${lowest})和最高位(${highest})数字伤害为治疗 ${conversion}。`;
      }

      // Calculate health after applying modified damage
      let currentAHealth = this.matchData.teamA_score;
      let currentBHealth = this.matchData.teamB_score;

      let healthAfterDamageA = currentAHealth - teamBModifiedDamageToA;
      let healthAfterDamageB = currentBHealth - teamAModifiedDamageToB;

      // Calculate raw overflow damage (before mirror)
      let rawOverflowDamageToA = healthAfterDamageA < 0 ? Math.abs(healthAfterDamageA) : 0;
      let rawOverflowDamageToB = healthAfterDamageB < 0 ? Math.abs(healthAfterDamageB) : 0;

      let teamAMirrorUsedThisTurn = false;
      let teamBMirrorUsedThisTurn = false;
      let teamAHealFromSupporterMirrorBonus = 0; // Additional heal from Supporter mirror
      let teamBHealFromSupporterMirrorBonus = 0;
      let teamAReflectedDamageByDefenderMirror = 0; // Damage reflected by Defender mirror
      let teamBReflectedDamageByDefenderMirror = 0;
      let teamAAttackerMirrorExtraDamage = 0; // Extra damage from Attacker mirror
      let teamBAttackerMirrorExtraDamage = 0;
      let teamAMirrorEffectLog = ''; // Log string for mirror effects
      let teamBMirrorEffectLog = '';

      // Start with health after damage, before mirror/healing
      let finalHealthA = healthAfterDamageA;
      let finalHealthB = healthAfterDamageB;

      // Check if mirrors can be triggered initially
      const canAInitiallyTriggerMirror = finalHealthA <= 0 && this.matchData.teamA_mirror_available;
      const canBInitiallyTriggerMirror = finalHealthB <= 0 && this.matchData.teamB_mirror_available;

      // Handle simultaneous mirror trigger
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
          // Handle individual mirror triggers (potential chain reaction)
          // Team A triggers first if eligible
          if (canAInitiallyTriggerMirror) {
              this.matchData.teamA_mirror_available = false;
              teamAMirrorUsedThisTurn = true;
              finalHealthA = MIRROR_HEALTH_RESTORE;
              teamAMirrorEffectLog = '触发复影折镜，血量恢复至20。';

              // Apply profession-specific mirror effects
              if (teamACurrentProfession === 'attacker') {
                  teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage;
                  finalHealthB -= teamAAttackerMirrorExtraDamage; // Deal extra damage to opponent
                  teamAMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamAAttackerMirrorExtraDamage}。`;
              } else if (teamACurrentProfession === 'defender') {
                  teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA;
                  finalHealthB -= teamAReflectedDamageByDefenderMirror; // Reflect overflow damage to opponent
                  teamAMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamAReflectedDamageByDefenderMirror}。`;
              } else if (teamACurrentProfession === 'supporter') {
                  teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill; // Additional heal based on base skill
                  // Supporter mirror doesn't deal damage, only heals self
                  teamAMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamAHealFromSupporterMirrorBonus}。`;
              }
          }

          // Team B triggers if eligible *after* A's potential mirror effect
          const canBTriggerAfterAPass1 = finalHealthB <= 0 && this.matchData.teamB_mirror_available && !teamBMirrorUsedThisTurn;
          if (canBTriggerAfterAPass1) {
              this.matchData.teamB_mirror_available = false;
              teamBMirrorUsedThisTurn = true;
              finalHealthB = MIRROR_HEALTH_RESTORE;
              teamBMirrorEffectLog = '触发复影折镜，血量恢复至20。';

              // Apply profession-specific mirror effects
              if (teamBCurrentProfession === 'attacker') {
                  teamBAttackerMirrorExtraDamage = teamBMaxDigitDamage;
                  finalHealthA -= teamBAttackerMirrorExtraDamage; // Deal extra damage to opponent
                  teamBMirrorEffectLog += ` 绝剑士折镜：追加最高位数字伤害 ${teamBAttackerMirrorExtraDamage}。`;
              } else if (teamBCurrentProfession === 'defender') {
                  teamBReflectedDamageByDefenderMirror = rawOverflowDamageToB;
                  finalHealthA -= teamBReflectedDamageByDefenderMirror; // Reflect overflow damage to opponent
                  teamBMirrorEffectLog += ` 矩盾手折镜：反弹对方溢出伤害 ${teamBReflectedDamageByDefenderMirror}。`;
              } else if (teamBCurrentProfession === 'supporter') {
                  teamBHealFromSupporterMirrorBonus = teamBHealFromSupporterSkill; // Additional heal based on base skill
                  // Supporter mirror doesn't deal damage, only heals self
                  teamBMirrorEffectLog += ` 炼星师折镜：额外治疗 ${teamBHealFromSupporterMirrorBonus}。`;
              }
          }

          // Team A triggers again if eligible *after* B's potential mirror effect (chain reaction)
          const canATriggerAfterBPass2 = finalHealthA <= 0 && this.matchData.teamA_mirror_available && !teamAMirrorUsedThisTurn;
          if (canATriggerAfterBPass2) {
              this.matchData.teamA_mirror_available = false;
              teamAMirrorUsedThisTurn = true;
              finalHealthA = MIRROR_HEALTH_RESTORE;
              teamAMirrorEffectLog = '触发复影折镜 (连锁反应)，血量恢复至20。'; // Indicate it was a chain reaction trigger

              // Apply profession-specific mirror effects again
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

      // Apply healing from Supporter base skill and mirror bonus
      finalHealthA += (teamAHealFromSupporterSkill + teamAHealFromSupporterMirrorBonus);
      finalHealthB += (teamBHealFromSupporterSkill + teamBHealFromSupporterMirrorBonus);

      // Apply effect values (positive or negative)
      finalHealthA += teamAEffectValue;
      finalHealthB += teamBEffectValue;

      // Store health before final rounding for summary
      const healthBeforeRoundingA = finalHealthA;
      const healthBeforeRoundingB = finalHealthB;

      // Round final health to nearest integer
      this.matchData.teamA_score = Math.round(finalHealthA);
      this.matchData.teamB_score = Math.round(finalHealthB);

      // Determine if either team is defeated
      const aDead = this.matchData.teamA_score <= 0;
      const bDead = this.matchData.teamB_score <= 0;

      // Determine the new match status
      let newStatus: MatchState['status'];
      let matchEnded = false;

      if (aDead && bDead) {
          // Both teams defeated simultaneously
          if (this.matchData.teamA_score > this.matchData.teamB_score) {
              newStatus = 'team_A_wins'; // A wins on score tiebreak
          } else if (this.matchData.teamB_score > this.matchData.teamA_score) {
              newStatus = 'team_B_wins'; // B wins on score tiebreak
          } else {
              newStatus = 'draw_pending_resolution'; // Exact score tie
          }
          matchEnded = true;
      } else if (aDead) {
          newStatus = 'team_B_wins';
          matchEnded = true;
      } else if (bDead) {
          newStatus = 'team_A_wins';
          matchEnded = true;
      } else {
          // No team defeated
          // Check if standard rounds are finished and it's not already a tiebreaker
          if (this.matchData.current_match_song_index >= STANDARD_ROUNDS_COUNT - 1 && !(this.matchData.current_song?.is_tiebreaker_song)) {
               // Finished standard rounds (index 5 is the 6th song), and current song is NOT a tiebreaker
               // Check scores to see if a tiebreaker is needed
               if (this.matchData.teamA_score === this.matchData.teamB_score) {
                    newStatus = 'tiebreaker_pending_song'; // Scores are tied, need tiebreaker song
               } else {
                    // Scores are not tied after standard rounds, match ends
                    newStatus = this.matchData.teamA_score > this.matchData.teamB_score ? 'team_A_wins' : 'team_B_wins';
                    matchEnded = true;
               }
          } else {
               // Standard rounds not finished, or it was a tiebreaker round that didn't end the match
               newStatus = 'round_finished'; // Ready to advance to the next round
          }
      }
      this.matchData.status = newStatus;

      // Update the current song's status and details in the match song list
      if (this.matchData.current_song) {
           this.matchData.current_song.status = 'completed';
           this.matchData.current_song.teamA_player_id = this.matchData.teamA_current_player_id ?? undefined;
           this.matchData.current_song.teamB_player_id = this.matchData.teamB_current_player_id ?? undefined;
           this.matchData.current_song.teamA_percentage = teamAPercentage;
           this.matchData.current_song.teamB_percentage = teamBPercentage;
           // Damage dealt is the health change inflicted on the opponent
           // NOTE: These fields in MatchSong might be intended to store the *actual* health change,
           // which is different from the damage dealt when healing/defense is involved.
           // The RoundSummary fields below are more appropriate for storing the calculated damage dealt.
           this.matchData.current_song.teamA_damage_dealt = currentBHealth - finalHealthB; // This is B's health loss
           this.matchData.current_song.teamB_damage_dealt = currentAHealth - finalHealthA; // This is A's health loss
           this.matchData.current_song.teamA_effect_value = teamAEffectValue;
           this.matchData.current_song.teamB_effect_value = teamBEffectValue;
           this.matchData.current_song.teamA_health_after = this.matchData.teamA_score;
           this.matchData.current_song.teamB_health_after = this.matchData.teamB_score;
           this.matchData.current_song.teamA_mirror_triggered = teamAMirrorUsedThisTurn;
           this.matchData.current_song.teamB_mirror_triggered = teamBMirrorUsedThisTurn;
      }


      // Create the round summary object
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
          teamA_modified_damage_to_B: teamAModifiedDamageToB, // Damage A deals to B, after A's skills
          teamB_modified_damage_to_A: teamBModifiedDamageToA, // Damage B deals to A, after B's skills

          teamA_health_before_round: currentAHealth,
          teamB_health_before_round: currentBHealth,

          teamA_mirror_triggered: teamAMirrorUsedThisTurn,
          teamB_mirror_triggered: teamBMirrorUsedThisTurn,
          teamA_mirror_effect_applied: teamAMirrorEffectLog,
          teamB_mirror_effect_applied: teamBMirrorEffectLog, // Corrected typo here
          teamA_supporter_base_skill_heal: teamAHealFromSupporterSkill,
          teamB_supporter_base_skill_heal: teamBHealFromSupporterSkill,
          teamA_supporter_mirror_bonus_heal: teamAHealFromSupporterMirrorBonus,
          teamB_supporter_mirror_bonus_heal: teamBHealFromSupporterMirrorBonus,

          // --- CORRECTED ASSIGNMENT FOR FINAL DAMAGE DEALT ---
          // These should reflect the damage *caused* by the player, after their own skills,
          // but before the opponent's defense/healing/mirror effects.
          teamA_final_damage_dealt: teamAModifiedDamageToB,
          teamB_final_damage_dealt: teamBModifiedDamageToA,
          // --- END CORRECTION ---

          teamA_health_change: this.matchData.teamA_score - currentAHealth, // Net health change for A
          teamB_health_change: this.matchData.teamB_score - currentBHealth, // Net health change for B
          teamA_health_after: this.matchData.teamA_score,
          teamB_health_after: this.matchData.teamB_score,

          is_tiebreaker_song: this.matchData.current_song?.is_tiebreaker_song ?? false,

          log: [], // Optional: Add detailed log steps here if needed
      };
      this.matchData.roundSummary = summary;


      try {
          // Save the updated state
          await this.state.storage.put('matchData', this.matchData);
          // Broadcast the updated state
          this.broadcast(this.matchData);

          // If the match ended, close WebSocket connections
          if (matchEnded || this.matchData.status === 'archived') {
              this.websockets.forEach(ws => ws.close(1000, `Match ended. Status: ${this.matchData.status}`));
              this.websockets = [];
          }
          console.log(`DO (${this.match_do_id}) Round ${this.matchData.current_match_song_index + 1} calculated. New status: ${this.matchData.status}`);
          return { success: true, message: `Round ${this.matchData.current_match_song_index + 1} calculated. New status: ${this.matchData.status}`, roundSummary: summary };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) Failed to save state after calculation:`, e);
          // If saving fails, set status to archived to prevent further actions
          this.matchData.status = 'archived';
          this.broadcast(this.matchData);
          return { success: false, message: `Failed to save state after calculation: ${e.message}` };
      }
  }


  // Archive the current round's data to D1 match_rounds_history table
  // Called automatically after calculateRoundOutcome if status is 'round_finished'
  // Or called explicitly by Worker if needed (e.g., after draw resolution)
  private async archiveCurrentRound(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
      if (!this.matchData || !this.matchData.roundSummary || !this.matchData.current_song) {
          return { success: false, message: "No match data, round summary, or current song to archive round." };
      }
      if (this.matchData.status === 'archived') {
          return { success: false, message: "Match is already archived, cannot archive rounds." };
      }

      const summary = this.matchData.roundSummary;
      const currentSong = this.matchData.current_song;

      // Ensure tournament_match_id is valid before attempting D1 write
      if (this.matchData.tournament_match_id === -1) {
           console.warn(`DO (${this.match_do_id}) Cannot archive round ${summary.round_number_in_match}: tournament_match_id is -1.`);
           return { success: false, message: "Match not linked to a tournament match ID." };
      }


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
              summary.teamA_final_damage_dealt, // Use the corrected field from summary
              summary.teamB_final_damage_dealt, // Use the corrected field from summary
              summary.teamA_health_change,
              summary.teamB_health_change,
              summary.teamA_health_before_round,
              summary.teamB_health_before_round,
              summary.teamA_health_after,
              summary.teamB_health_after,
              summary.teamA_mirror_triggered ? 1 : 0, // Store boolean as integer 1 or 0
              summary.teamB_mirror_triggered ? 1 : 0,
              summary.teamA_effect_value_applied,
              summary.teamB_effect_value_applied,
              summary.is_tiebreaker_song ? 1 : 0, // Store boolean as integer 1 or 0
              new Date().toISOString(),
              JSON.stringify(summary) // Store the full summary JSON
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
  // Called by the Worker after the frontend confirms the round summary.
  private async nextRound(): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to advance round." };
      }
      if (this.matchData.status !== 'round_finished') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'round_finished' to advance.` };
      }

      // Archive the current round's data before advancing
      const archiveResult = await this.archiveCurrentRound();
      if (!archiveResult.success) {
          console.warn(`DO (${this.match_do_id}) Failed to auto-archive current round ${this.matchData.current_match_song_index + 1} before advancing:`, archiveResult.message);
          // Decide if you want to block advancing if archiving fails.
          // For now, we'll log a warning and proceed, assuming archiving can be retried or isn't critical path.
      }

      const nextSongIndex = this.matchData.current_match_song_index + 1;

      // Check if there is a next song in the list
      if (nextSongIndex >= this.matchData.match_song_list.length) {
           const msg = `Cannot advance round. No song found at index ${nextSongIndex}. Match song list length: ${this.matchData.match_song_list.length}.`;
           console.error(`DO (${this.match_do_id}): ${msg}`);
           // If we ran out of songs and the match hasn't ended by health, it's likely a draw or unexpected state.
           // Transition to a final state if not already there.
           if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'tiebreaker_pending_song', 'archived'].includes(this.matchData.status)) {
                this.matchData.status = 'completed'; // Or 'draw_pending_resolution' if scores are tied?
                await this.state.storage.put('matchData', this.matchData);
                this.broadcast(this.matchData);
           }
           return { success: false, message: msg };
      }

      // Update state for the next round
      this.matchData.current_match_song_index = nextSongIndex;
      this.matchData.current_song = this.matchData.match_song_list[nextSongIndex];
      this.matchData.current_song.status = 'ongoing'; // Set the new current song status

      // Determine players for the next round
      const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
      const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
      const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

      this.matchData.teamA_current_player_id = playerAId;
      this.matchData.teamB_current_player_id = playerBId;
      this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
      this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
      this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
      this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);

      // Clear the round summary for the new round
      this.matchData.roundSummary = null;
      // Set status back to pending_scores for the new round
      this.matchData.status = 'pending_scores';

      try {
          // Save the updated state
          await this.state.storage.put('matchData', this.matchData);
          // Broadcast the updated state
          this.broadcast(this.matchData);
          console.log(`DO (${this.match_do_id}) advanced to Round ${this.matchData.current_match_song_index + 1}`);
          return { success: true, message: `Advanced to Round ${this.matchData.current_match_song_index + 1}` };
      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) failed to advance round:`, e);
          // If saving fails, set status to archived to prevent further actions
          this.matchData.status = 'archived';
          this.broadcast(this.matchData);
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

          // Construct the MatchSong object for the tiebreaker
          const tiebreakerSong: MatchSong = {
              song_id: song.id,
              song_title: song.title,
              song_difficulty: fullDifficultyString,
              // Example mapping for element based on category (adjust as needed)
              song_element: song.category === 'original' ? 'fire' : song.category === 'niconico' ? 'wood' : null,
              cover_filename: song.cover_filename,
              bpm: song.bpm,
              // Construct fullCoverUrl using the R2 bucket name
              fullCoverUrl: song.cover_filename && this.env.SONG_COVER_BUCKET?.name
                  ? `https://${this.env.SONG_COVER_BUCKET.name}.r2.dev/${song.cover_filename}`
                  : undefined,

              // Assign picker info (can use a special ID for Staff/System)
              // TODO: Define a Staff/System member ID or team ID for tiebreakers
              picker_member_id: -1, // Placeholder for Staff/System
              picker_team_id: -1, // Placeholder for Staff/System
              is_tiebreaker_song: true, // Mark as tiebreaker

              status: 'pending', // Status starts as pending
          };

          // Add the tiebreaker song to the end of the match song list
          this.matchData.match_song_list.push(tiebreakerSong);

          // Advance the index to the newly added tiebreaker song
          this.matchData.current_match_song_index = this.matchData.match_song_list.length - 1;
          this.matchData.current_song = tiebreakerSong;
          this.matchData.current_song.status = 'ongoing'; // Set status to ongoing

          // Determine players for the tiebreaker round (usually continues the cycle)
          const { playerAId, playerBId } = this.getCurrentPlayers(this.matchData);
          const memberA = this.getMemberById(playerAId, this.matchData.teamA_members);
          const memberB = this.getMemberById(playerBId, this.matchData.teamB_members);

          this.matchData.teamA_current_player_id = playerAId;
          this.matchData.teamB_current_player_id = playerBId;
          this.matchData.teamA_current_player_nickname = memberA?.nickname || '未知选手';
          this.matchData.teamB_current_player_nickname = memberB?.nickname || '未知选手';
          this.matchData.teamA_current_player_profession = this.getInternalProfession(memberA?.job);
          this.matchData.teamB_current_player_profession = this.getInternalProfession(memberB?.job);


          // Clear the round summary for the new round
          this.matchData.roundSummary = null;
          // Set status back to pending_scores for the tiebreaker round
          this.matchData.status = 'pending_scores';

          // Save and broadcast the updated state
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);

          console.log(`DO (${this.match_do_id}) Tiebreaker song selected. Advanced to Round ${this.matchData.current_match_song_index + 1}`);
          return { success: true, message: `Tiebreaker song selected. Advanced to Round ${this.matchData.current_match_song_index + 1}` };

      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) exception selecting tiebreaker song:`, e);
          // If saving fails, set status to archived to prevent further actions
          this.matchData.status = 'archived';
          this.broadcast(this.matchData);
          return { success: false, message: `Failed to select tiebreaker song: ${e.message}` };
      }
  }


  // Archive the entire match summary and rounds to D1
  // Called by the Worker when the match is finalized (e.g., after a win/loss or draw resolution).
  private async archiveMatch(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to archive match." };
      }
      if (this.matchData.status === 'archived') {
          return { success: true, message: "Match already archived.", d1RecordId: this.match_do_id };
      }
      // Log a warning if archiving from a non-final state
      if (!['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'completed'].includes(this.matchData.status)) {
           console.warn(`DO (${this.match_do_id}): Archiving match from non-final state: ${this.matchData.status}`);
      }

      // Ensure tournament_match_id is valid before attempting D1 write
      if (this.matchData.tournament_match_id === -1) {
           console.warn(`DO (${this.match_do_id}) Cannot archive match: tournament_match_id is -1.`);
           // Still proceed to archive DO state and close WS, but skip D1 update
           // return { success: false, message: "Match not linked to a tournament match ID." }; // Decide if this should be a hard error
      }


      try {
          // Ensure the last round's summary is archived to D1 history table
          // This is important if the match ended immediately after calculateRoundOutcome
          // without going through the 'round_finished' -> 'nextRound' flow.
          if (this.matchData.roundSummary && this.matchData.roundSummary.round_number_in_match === this.matchData.current_match_song_index + 1) {
               const archiveRoundResult = await this.archiveCurrentRound();
               if (!archiveRoundResult.success) {
                   console.error(`DO (${this.match_do_id}) Failed to archive final round ${this.matchData.current_match_song_index + 1} during match archive:`, archiveRoundResult.message);
                   // Decide if this failure should prevent archiving the match summary
                   // For now, we'll log and continue.
               }
          }
          // Note: If there are multiple completed rounds that haven't been archived yet
          // (e.g., due to previous errors), you might need logic here to archive them all.
          // The current flow assumes rounds are archived one by one via nextRound or this final archive.


          // Update the tournament_matches record in D1 with final status and scores
          if (this.matchData.tournament_match_id && this.matchData.tournament_match_id !== -1) {
              try {
                  const winnerTeamId = this.determineWinnerTeamId(this.matchData);

                  // Map DO status to D1 tournament_matches status
                  const tournamentMatchStatus = ['team_A_wins', 'team_B_wins'].includes(this.matchData.status) ? 'completed'
                                                  : this.matchData.status === 'draw_pending_resolution' ? 'completed' // Draw is also a completed state
                                                  : 'archived'; // Any other state when archiving is just archived


                  const updateTournamentStmt = this.env.DB.prepare(
                      `UPDATE tournament_matches SET
                         status = ?,
                         winner_team_id = ?,
                         final_score_team1 = ?,
                         final_score_team2 = ?,
                         match_do_id = ?, -- Store the DO ID in the D1 record
                         updated_at = ?
                       WHERE id = ?`
                  );
                  const updateTournamentResult = await updateTournamentStmt.bind(
                      tournamentMatchStatus,
                      winnerTeamId,
                      this.matchData.teamA_score,
                      this.matchData.teamB_score,
                      this.match_do_id, // Save the DO ID
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
                  // Decide if this failure should prevent archiving the DO state
                  // For now, we'll log and continue.
              }
          }


          // Set the DO's internal state to archived
          this.matchData.status = 'archived';
          // Save the final state to storage
          await this.state.storage.put('matchData', this.matchData);
          // Broadcast the final state
          this.broadcast(this.matchData);

          // Close all WebSocket connections
          this.websockets.forEach(ws => ws.close(1000, "Match archived and finished."));
          this.websockets = [];

          console.log(`DO (${this.match_do_id}) Match archived.`);
          // Return success even if D1 update failed, as the DO state is archived
          return { success: true, message: "Match data archived.", d1RecordId: this.match_do_id };


      } catch (e: any) {
          console.error(`DO (${this.match_do_id}) exception during D1 match archive:`, e);
          // If saving DO state fails, it's a critical error
          return { success: false, message: `Exception during match archive: ${e.message}` };
      }
  }

  // Resolve a draw by setting the winner
  // Called by the Worker after Staff selects a winner for a draw.
  private async resolveDraw(winnerDesignation: 'teamA' | 'teamB'): Promise<{ success: boolean; message?: string }> {
      if (!this.matchData) {
          return { success: false, message: "No match data to resolve draw." };
      }
      if (this.matchData.status !== 'draw_pending_resolution') {
          return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'draw_pending_resolution' to resolve.` };
      }

      console.log(`DO (${this.match_do_id}) Resolving draw. Winner: ${winnerDesignation}`);

      // Set the final status based on the winner designation
      if (winnerDesignation === 'teamA') {
          this.matchData.status = 'team_A_wins';
          // Optionally set opponent's score to 0 if they lost the tiebreak
          if (this.matchData.teamB_score > 0) this.matchData.teamB_score = 0;
      } else if (winnerDesignation === 'teamB') {
          this.matchData.status = 'team_B_wins';
          // Optionally set opponent's score to 0 if they lost the tiebreak
          if (this.matchData.teamA_score > 0) this.matchData.teamA_score = 0;
      } else {
          return { success: false, message: "Invalid winner designation." };
      }

      // Archive the match after resolving the draw
      const archiveResult = await this.archiveMatch();
      if (!archiveResult.success) {
          console.error(`DO (${this.match_do_id}) Failed to auto-archive match after draw resolution:`, archiveResult.message);
          // Decide if this failure should prevent returning success for draw resolution
          // For now, we'll log and return failure.
          return { success: false, message: `Draw resolved, but failed to archive match: ${archiveResult.message}` };
      }

      console.log(`DO (${this.match_do_id}) Draw resolved. New status: ${this.matchData.status}`);
      return { success: true, message: `Draw resolved. ${this.matchData.status.replace('_', ' ')}.` };
  }


  // --- Durable Object Fetch Handler ---
  // This method receives requests forwarded from the Worker.
  async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);

      // Handle WebSocket upgrade requests
      if (url.pathname === '/websocket') {
          if (request.headers.get('Upgrade') !== 'websocket') {
              return new Response('Expected Upgrade: websocket', { status: 426 });
          }
          // Create a WebSocketPair and accept the connection
          const [client, server] = Object.values(new WebSocketPair());
          this.websockets.push(server); // Add the server end to our list
          server.accept();
          console.log(`DO (${this.match_do_id}) WebSocket connected. Total: ${this.websockets.length}`);

          // Send the current state immediately upon connection
          if (this.matchData) {
               server.send(JSON.stringify(this.matchData));
          } else {
               // Should not happen if Worker initializes correctly, but handle defensively
               server.send(JSON.stringify({ success: false, error: "Match data not initialized in DO" }));
          }

          // Add event listeners for messages, close, and errors
          server.addEventListener('message', event => {
              console.log(`DO (${this.match_do_id}) WS message from client:`, event.data);
              // TODO: Handle incoming messages from clients if needed (e.g., chat, player actions)
          });
          server.addEventListener('close', (event) => {
              console.log(`DO (${this.match_do_id}) WebSocket closed. Code: ${event.code}, Reason: ${event.reason}`);
              // Remove the closed socket from the list
              this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
              console.log(`DO (${this.match_do_id}) WebSocket disconnected. Remaining: ${this.websockets.length}`);
          });
          server.addEventListener('error', (err) => {
              console.error(`DO (${this.match_do_id}) WebSocket error:`, err);
              // Remove the socket on error
              this.websockets = this.websockets.filter(ws => ws !== server);
              console.log(`DO (${this.match_do_id}) WebSocket error disconnected. Remaining: ${this.websockets.length}`);
          });

          // Return the client end of the WebSocketPair
          return new Response(null, { status: 101, webSocket: client });
      }

      // Handle HTTP GET request for the current state
      if (url.pathname === '/state' && request.method === 'GET') {
           if (!this.matchData) {
               return new Response(JSON.stringify({ success: false, error: "Match data not initialized in DO" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
           }
          // Return the current match state as JSON
          return new Response(JSON.stringify(this.matchData), {
              headers: { 'Content-Type': 'application/json' },
          });
      }

      // --- Internal Endpoints for Actions (Called by Worker) ---
      // These endpoints are typically called by the Worker to trigger state changes.

      // Internal endpoint to initialize the DO state from schedule data
      if (url.pathname === '/internal/initialize-from-schedule' && request.method === 'POST') {
          try {
              const scheduleData = await request.json<MatchScheduleData>();
               // Basic validation for the payload structure
               if (!scheduleData || scheduleData.tournamentMatchId === undefined || !scheduleData.round_name || scheduleData.team1_id === undefined || scheduleData.team2_id === undefined || !scheduleData.team1_name || !scheduleData.team2_name || !Array.isArray(scheduleData.team1_members) || !Array.isArray(scheduleData.team2_members) || !Array.isArray(scheduleData.team1_player_order_ids) || !Array.isArray(scheduleData.team2_player_order_ids) || !Array.isArray(scheduleData.match_song_list)) {
                   return new Response(JSON.stringify({ success: false, error: "Invalid schedule data payload" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
               }

              const initResult = await this.initializeFromSchedule(scheduleData);
              if (initResult.success) {
                  return new Response(JSON.stringify({ success: true, message: initResult.message }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  // Return error response if initialization failed
                  return new Response(JSON.stringify({ success: false, error: initResult.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing initialize-from-schedule payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid initialize-from-schedule payload', details: e.message }), { status: 400 });
          }
      }

      // Require match data to be initialized for subsequent actions
      if (!this.matchData || this.matchData.tournament_match_id === -1) {
           return new Response(JSON.stringify({ success: false, error: "Match is not initialized from schedule." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
      }


      // Internal endpoint to calculate the outcome of the current round
      if (url.pathname === '/internal/calculate-round' && request.method === 'POST') {
          try {
              const payload = await request.json<CalculateRoundPayload>();
              // Validate payload
              if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
                  return new Response(JSON.stringify({ success: false, error: "Invalid calculate-round payload: percentages must be numbers." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
              const result = await this.calculateRoundOutcome(payload);
              if (result.success) {
                  // Return success response, including the round summary
                  return new Response(JSON.stringify({ success: true, message: result.message, roundSummary: result.roundSummary }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  // Return error response
                  const status = result.message?.includes("Cannot calculate round") ? 400 : 500; // Use 400 for status-related errors
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: status, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing calculate-round payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid calculate-round payload', details: e.message }), { status: 400 });
          }
      }

      // Internal endpoint to archive the current round's data to D1
      // This is called by the Worker, often after calculate-round and before next-round.
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
              // Return 400 if advancing is not possible due to status
              return new Response(JSON.stringify({ success: false, error: nextRoundResult.message }), { status: 400, headers: { 'Content-Type': 'application/json' } });
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
              // Validate payload
              if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
                  return new Response(JSON.stringify({ success: false, error: "Invalid resolve-draw payload: winner must be 'teamA' or 'teamB'." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
              const result = await this.resolveDraw(payload.winner);
              if (result.success) {
                  return new Response(JSON.stringify({ success: true, message: result.message }), { headers: { 'Content-Type': 'application/json' } });
              } else {
                  // Return 400 if draw resolution is not possible due to status
                  return new Response(JSON.stringify({ success: false, error: result.message }), { status: 400, headers: { 'Content-Type': 'application/json' } });
              }
          } catch (e: any) {
              console.error(`DO (${this.match_do_id}) Exception processing resolve_draw payload:`, e);
              return new Response(JSON.stringify({ success: false, error: 'Invalid resolve-draw payload', details: e.message }), { status: 400 });
          }
      }

      // Internal endpoint for Staff to select a tiebreaker song
      // This now expects the payload to include song_details fetched by the Worker
      if (url.pathname === '/internal/select-tiebreaker-song' && request.method === 'POST') {
           try {
               // The payload now includes song_details fetched by the Worker
               interface InternalSelectTiebreakerPayload extends SelectTiebreakerSongPayload {
                   song_details: Song;
               }
               const payload = await request.json<InternalSelectTiebreakerPayload>();

               // Validate payload, including the presence and basic structure of song_details
               if (typeof payload.song_id !== 'number' || typeof payload.selected_difficulty !== 'string' || !payload.song_details || typeof payload.song_details.id !== 'number') {
                   return new Response(JSON.stringify({ success: false, error: "Invalid select-tiebreaker-song payload: song_id (number), selected_difficulty (string), and song_details (Song) are required." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
               }
               // Optional: Basic check that song_id matches song_details.id
               if (payload.song_id !== payload.song_details.id) {
                    console.warn(`DO (${this.match_do_id}): select_tiebreaker_song payload song_id (${payload.song_id}) does not match song_details.id (${payload.song_details.id}). Using song_details.`);
                    // Decide how to handle this discrepancy - using song_details is safer
               }


               const result = await this.selectTiebreakerSong(payload); // Pass the payload including song_details
               if (result.success) {
                   return new Response(JSON.stringify({ success: true, message: result.message }), { headers: { 'Content-Type': 'application/json' } });
               } else {
                   // Return 400 if selecting tiebreaker is not possible due to status
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
