// src/durable-objects/matchDo.ts
import type { MatchState, Env, RoundArchive, MatchArchiveSummary, TournamentMatch, Team, Member, MatchScheduleData, CalculateRoundPayload, ResolveDrawPayload, InternalProfession } from '../types';

// Constants for game logic
const INITIAL_HEALTH = 100;
const MIRROR_HEALTH_RESTORE = 20;
const MAX_DAMAGE_DIGIT = 10; // 0% completion corresponds to 10 damage

// Default state for a new match (fallback, initialization should come from schedule)
const defaultMatchState: Omit<MatchState, 'matchId'> = {
  tournamentMatchId: null,
  round: 1,
  teamA_name: 'Team A',
  teamA_score: INITIAL_HEALTH, // Default initial health
  teamA_player: 'Player A1',
  teamB_name: 'Team B',
  teamB_score: INITIAL_HEALTH, // Default initial health
  teamB_player: 'Player B1',
  teamA_members: [],
  teamB_members: [],
  teamA_player_order_ids: [],
  teamB_player_order_ids: [],
  current_player_index_a: 0,
  current_player_index_b: 0,
  teamA_mirror_available: true, // Default mirror available
  teamB_mirror_available: true, // Default mirror available
  teamA_current_player_profession: null, // Will be set on initialization
  teamB_current_player_profession: null, // Will be set on initialization
  status: 'pending',
};

export class MatchDO implements DurableObject {
  state: DurableObjectState;
  env: Env;
  matchData: MatchState | null = null;
  matchId: string;
  websockets: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.matchId = state.id.toString();

    // Load state from storage on DO startup
    this.state.blockConcurrencyWhile(async () => {
      const storedMatchData = await this.state.storage.get<MatchState>('matchData');
      if (storedMatchData) {
        this.matchData = storedMatchData;
        // Ensure new fields are initialized if loading old state without them
        // This is a basic migration step. More complex migrations might be needed.
        if (this.matchData && this.matchData.teamA_members === undefined) {
             console.warn(`DO (${this.matchId}): Initializing new fields for old state.`);
             this.matchData.tournamentMatchId = this.matchData.tournamentMatchId ?? null;
             this.matchData.teamA_members = this.matchData.teamA_members ?? [];
             this.matchData.teamB_members = this.matchData.teamB_members ?? [];
             this.matchData.teamA_player_order_ids = this.matchData.teamA_player_order_ids ?? [];
             this.matchData.teamB_player_order_ids = this.matchData.teamB_player_order_ids ?? [];
             this.matchData.current_player_index_a = this.matchData.current_player_index_a ?? 0;
             this.matchData.current_player_index_b = this.matchData.current_player_index_b ?? 0;
             this.matchData.teamA_mirror_available = this.matchData.teamA_mirror_available ?? true; // Assume mirror available for old matches
             this.matchData.teamB_mirror_available = this.matchData.teamB_mirror_available ?? true; // Assume mirror available for old matches
             this.matchData.teamA_current_player_profession = this.matchData.teamA_current_player_profession ?? null;
             this.matchData.teamB_current_player_profession = this.matchData.teamB_current_player_profession ?? null;
             // Player names might be placeholders from old state, will be updated if initialized from schedule
             // Status might need mapping if old states used different strings
             this.matchData.status = this.matchData.status as MatchState['status'] ?? 'pending';

             await this.state.storage.put('matchData', this.matchData); // Persist updated structure
        }
      } else {
        // DO is being created for the first time for this ID.
        // It should ideally be initialized via /internal/initialize-from-schedule.
        // Initialize with default state as a fallback, but log a warning.
        console.warn(`DO (${this.matchId}): Initializing with default state. Should ideally be initialized from schedule.`);
        this.matchData = { ...defaultMatchState, matchId: this.matchId };
        // No need to await put here, initializeFromSchedule or first update will save it.
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
      console.warn(`DO (${this.matchId}): Unknown profession string "${job}". Returning null.`);
      return null;
  }

  // Helper to get a member by ID from a list
  private getMemberById(memberId: number | undefined, members: Member[] | undefined): Member | undefined {
      if (memberId === undefined || members === undefined) {
          return undefined;
      }
      return members.find(m => m.id === memberId);
  }

  // Helper to get player nickname based on member ID and members list
  private getPlayerNickname(memberId: number | undefined, members: Member[] | undefined): string {
      const member = this.getMemberById(memberId, members);
      return member?.nickname || '未知选手';
  }

  // Helper to parse damage digits from percentage (0-101.0000)
  private parseDamageDigits(percentage: number): number[] {
    // Ensure percentage is within a reasonable range (e.g., 0 to 101.0000 for maimai)
    // Allow up to 101.0000, slightly more for floating point comparisons if needed, but toFixed will handle it.
    const clampedPercentage = Math.max(0, Math.min(101.0000, percentage));

    // Convert to string with exactly 4 decimal places
    // For numbers like 101 (integer), toFixed(4) will produce "101.0000"
    // For numbers like 95.5, toFixed(4) will produce "95.5000"
    const percentageString = clampedPercentage.toFixed(4);
    const parts = percentageString.split('.');

    // This condition should still hold: we expect a decimal part.
    // If clampedPercentage was an integer like 101, parts[1] will be "0000".
    if (parts.length !== 2) {
        console.error(`DO (${this.matchId}): Unexpected percentage format after toFixed: ${percentageString} (original: ${percentage})`);
        return [MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT, MAX_DAMAGE_DIGIT]; // Return max damage on error or all 10s
    }

    const digitsString = parts[1]; // Get the decimal part as string (e.g., "2345" from "101.2345" or "0000" from "101.0000")
    const digits: number[] = [];
    for (let i = 0; i < 4; i++) {
        // Get digit or '0' if string is too short (should not happen with toFixed(4))
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

  private determineWinner(state: { teamA_score: number; teamB_score: number; teamA_name: string; teamB_name: string }): string | null {
      if (state.teamA_score > state.teamB_score) {
          return state.teamA_name || '队伍A';
      } else if (state.teamB_score > state.teamA_score) {
          return state.teamB_name || '队伍B';
      } else {
          return null; // Draw or undecided
      }
  }

  // --- Internal Method: Initialize from Schedule ---
  // Called by the Worker when starting a match from the schedule
  private async initializeFromSchedule(scheduleData: MatchScheduleData): Promise<{ success: boolean; message?: string }> {
      console.log(`DO (${this.matchId}): Initializing from schedule for tournament match ${scheduleData.tournamentMatchId}`);

      // Clear existing state if any, only if it's not already a live match for this ID
      if (this.matchData?.tournamentMatchId !== scheduleData.tournamentMatchId || this.matchData?.status === 'archived_in_d1') {
         await this.state.storage.deleteAll();
         console.log(`DO (${this.matchId}): Cleared storage for new initialization.`);
      } else {
         console.log(`DO (${this.matchId}): Already initialized for tournament match ${scheduleData.tournamentMatchId}. Skipping storage clear.`);
      }


      // Validate player orders and members
      const validateOrder = (orderIds: number[], members: Member[], teamName: string) => {
          if (!Array.isArray(orderIds) || orderIds.length === 0) {
              console.error(`DO (${this.matchId}): Invalid or empty player order for ${teamName}.`);
              return false;
          }
          for (const id of orderIds) {
              if (!members.find(m => m.id === id)) {
                  console.error(`DO (${this.matchId}): Player ID ${id} in order for ${teamName} not found in member list.`);
                  return false;
              }
          }
          return true;
      };

      if (!validateOrder(scheduleData.team1_player_order_ids, scheduleData.team1_members, scheduleData.team1_name) ||
          !validateOrder(scheduleData.team2_player_order_ids, scheduleData.team2_members, scheduleData.team2_name)) {
          const msg = "Invalid player order or member data provided for initialization.";
          console.error(`DO (${this.matchId}): ${msg}`);
          return { success: false, message: msg };
      }


      // Initialize matchData from schedule data
      this.matchData = {
          matchId: this.matchId,
          tournamentMatchId: scheduleData.tournamentMatchId,
          round: 1, // Always start at round 1 for a new live match
          teamA_name: scheduleData.team1_name,
          teamA_score: INITIAL_HEALTH, // Start with full health
          teamA_player: this.getPlayerNickname(scheduleData.team1_player_order_ids[0], scheduleData.team1_members), // Get 1st player nickname
          teamB_name: scheduleData.team2_name,
          teamB_score: INITIAL_HEALTH, // Start with full health
          teamB_player: this.getPlayerNickname(scheduleData.team2_player_order_ids[0], scheduleData.team2_members), // Get 1st player nickname
          teamA_members: scheduleData.team1_members,
          teamB_members: scheduleData.team2_members,
          teamA_player_order_ids: scheduleData.team1_player_order_ids,
          teamB_player_order_ids: scheduleData.team2_player_order_ids,
          current_player_index_a: 0, // Start with the first player
          current_player_index_b: 0, // Start with the first player
          teamA_mirror_available: true, // Mirror available at start
          teamB_mirror_available: true, // Mirror available at start
          teamA_current_player_profession: this.getInternalProfession(
              this.getMemberById(scheduleData.team1_player_order_ids[0], scheduleData.team1_members)?.job
          ),
          teamB_current_player_profession: this.getInternalProfession(
              this.getMemberById(scheduleData.team2_player_order_ids[0], scheduleData.team2_members)?.job
          ),
          status: 'pending', // Start as pending, waiting for scores
      };

      try {
          await this.state.storage.put('matchData', this.matchData);
          this.broadcast(this.matchData);
          console.log(`DO (${this.matchId}): State initialized from schedule.`);
          return { success: true, message: "Match initialized from schedule." };
      } catch (e: any) {
          console.error(`DO (${this.matchId}): Failed to save initial state from schedule:`, e);
          return { success: false, message: `Failed to initialize match: ${e.message}` };
      }
  }

  // --- Core Game Logic: Calculate Round Outcome ---
  private async calculateRoundOutcome(payload: CalculateRoundPayload): Promise<{ success: boolean; message?: string; roundSummary?: any }> {
    if (!this.matchData) {
        const msg = "Match data not initialized.";
        console.error(`DO (${this.matchId}): ${msg}`);
        return { success: false, message: msg };
    }
    if (['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'archived_in_d1'].includes(this.matchData.status)) {
        const msg = `Match is already in a final state: ${this.matchData.status}. Cannot calculate round.`;
        console.warn(`DO (${this.matchId}): ${msg}`);
        return { success: false, message: msg };
    }
     if (this.matchData.status !== 'pending') {
         const msg = `Match is not in 'pending' status (${this.matchData.status}). Cannot calculate round.`;
         console.warn(`DO (${this.matchId}): ${msg}`);
         return { success: false, message: msg };
     }


    console.log(`DO (${this.matchId}) Calculating Round ${this.matchData.round} with A: ${payload.teamA_percentage}%, B: ${payload.teamB_percentage}%`);

    // --- 1. Parse Damage & Get Professions ---
    // Validate percentages are numbers
    const teamAPercentage = typeof payload.teamA_percentage === 'number' ? payload.teamA_percentage : 0;
    const teamBPercentage = typeof payload.teamB_percentage === 'number' ? payload.teamB_percentage : 0;

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

    // --- 2. Apply Profession Effects (Pre-damage/Modification) ---
    let teamAModifiedDamageToB = teamABaseDamage; // Damage A will deal to B
    let teamBModifiedDamageToA = teamBBaseDamage; // Damage B will deal to A
    let teamAHealFromSupporterSkill = 0; // Supporter's *base* skill heal (converted damage)
    let teamBHealFromSupporterSkill = 0; // Supporter's *base* skill heal (converted damage)

    // Attacker: Additional damage
    if (teamACurrentProfession === 'attacker') {
        teamAModifiedDamageToB += teamAMaxDigitDamage;
        console.log(`A Attacker skill adds ${teamAMaxDigitDamage} damage to B.`);
    }
    if (teamBCurrentProfession === 'attacker') {
        teamBModifiedDamageToA += teamBMaxDigitDamage;
        console.log(`B Attacker skill adds ${teamBMaxDigitDamage} damage to A.`);
    }

    // Defender: Invalidate one random opponent damage digit
    if (teamACurrentProfession === 'defender' && teamBDamageDigits.length > 0) {
        const randomIndex = Math.floor(Math.random() * teamBDamageDigits.length);
        const invalidatedDamage = teamBDamageDigits[randomIndex];
        teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - invalidatedDamage); // Ensure damage doesn't go negative
        console.log(`A Defender skill invalidates B's digit ${randomIndex + 1} (${invalidatedDamage} damage). B damage to A is now ${teamBModifiedDamageToA}.`);
    }
    if (teamBCurrentProfession === 'defender' && teamADamageDigits.length > 0) {
        const randomIndex = Math.floor(Math.random() * teamADamageDigits.length);
        const invalidatedDamage = teamADamageDigits[randomIndex];
        teamAModifiedDamageToB = Math.max(0, teamAModifiedDamageToB - invalidatedDamage); // Ensure damage doesn't go negative
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
        console.log(`A Supporter skill converts ${conversion} damage to heal. A damage to B is now ${teamAModifiedDamageToB}.`);
    }
    if (teamBCurrentProfession === 'supporter' && teamBDamageDigits.length >= 2) {
        const sortedDigits = [...teamBDamageDigits].sort((a, b) => a - b);
        const lowest = sortedDigits[0];
        const highest = sortedDigits[sortedDigits.length - 1];
        const conversion = lowest + highest;
        teamBModifiedDamageToA = Math.max(0, teamBModifiedDamageToA - conversion);
        teamBHealFromSupporterSkill += conversion;
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
            console.log(`A Mirror consumed. A Health set to ${finalHealthA}.`);

            // Apply A's profession-specific Mirror effect
            if (teamACurrentProfession === 'attacker') {
                teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage;
                finalHealthB -= teamAAttackerMirrorExtraDamage; // Damage B
                console.log(`A Attacker Mirror adds ${teamAAttackerMirrorExtraDamage} damage to B. B health now potentially ${finalHealthB}.`);
            } else if (teamACurrentProfession === 'defender') {
                teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA; // A's Defender reflects B's *original* overflow to A
                finalHealthB -= teamAReflectedDamageByDefenderMirror;
                console.log(`A Defender Mirror reflects ${teamAReflectedDamageByDefenderMirror} (original overflow B caused to A) back to B. B health now potentially ${finalHealthB}.`);
            } else if (teamACurrentProfession === 'supporter') {
                teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill; // Supporter mirror *bonus* is equal to base skill heal
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
            console.log(`B Mirror consumed. B Health set to ${finalHealthB}.`);

            // Apply B's profession-specific Mirror effect
            if (teamBCurrentProfession === 'attacker') {
                teamBAttackerMirrorExtraDamage = teamBMaxDigitDamage;
                finalHealthA -= teamBAttackerMirrorExtraDamage; // Damage A
                console.log(`B Attacker Mirror adds ${teamBAttackerMirrorExtraDamage} damage to A. A health now potentially ${finalHealthA}.`);
            } else if (teamBCurrentProfession === 'defender') {
                teamBReflectedDamageByDefenderMirror = rawOverflowDamageToB; // B's Defender reflects A's *original* overflow to B (Corrected)
                finalHealthA -= teamBReflectedDamageByDefenderMirror;
                console.log(`B Defender Mirror reflects ${teamBReflectedDamageByDefenderMirror} (original overflow A caused to B) back to A. A health now potentially ${finalHealthA}.`);
            } else if (teamBCurrentProfession === 'supporter') {
                teamBHealFromSupporterMirrorBonus = teamBHealFromSupporterSkill; // Supporter mirror *bonus*
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
            console.log(`A Mirror consumed (Chain Reaction). A Health set to ${finalHealthA}.`);

            // Apply A's profession-specific Mirror effect (this is A's *only* mirror effect application)
            if (teamACurrentProfession === 'attacker') {
                teamAAttackerMirrorExtraDamage = teamAMaxDigitDamage; // Re-set/confirm bonus damage
                finalHealthB -= teamAAttackerMirrorExtraDamage; // Damage B (B might have already used its mirror)
                console.log(`A Attacker Mirror (Chain Reaction) adds ${teamAAttackerMirrorExtraDamage} damage to B. B health now potentially ${finalHealthB}.`);
            } else if (teamACurrentProfession === 'defender') {
                // If A is defender, it reflects B's overflow.
                // rawOverflowDamageToA is B's *initial* overflow to A.
                // If B's mirror effect (e.g. attacker) caused new "overflow" to A, that's not what's reflected.
                // Defender reflects the *opponent's initial attack's overflow*.
                teamAReflectedDamageByDefenderMirror = rawOverflowDamageToA;
                finalHealthB -= teamAReflectedDamageByDefenderMirror;
                console.log(`A Defender Mirror (Chain Reaction) reflects ${teamAReflectedDamageByDefenderMirror} (original overflow B caused to A) back to B. B health now potentially ${finalHealthB}.`);
            } else if (teamACurrentProfession === 'supporter') {
                // If A's mirror is used here for the first time, set the bonus.
                // If it was already set (e.g. if this chain logic was more complex), ensure it's not doubled.
                // Since it's a single use, this is fine.
                teamAHealFromSupporterMirrorBonus = teamAHealFromSupporterSkill;
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

    // --- 6. Update Match State and Check Match End Condition ---
    this.matchData.teamA_score = Math.round(finalHealthA); // Round to nearest integer for health
    this.matchData.teamB_score = Math.round(finalHealthB); // Round to nearest integer for health

    const aDead = this.matchData.teamA_score <= 0;
    const bDead = this.matchData.teamB_score <= 0;

    if (aDead && bDead) {
        // Both are dead. Determine winner based on final score.
        if (this.matchData.teamA_score > this.matchData.teamB_score) {
             this.matchData.status = 'team_A_wins'; // A has higher (less negative) score
        } else if (this.matchData.teamB_score > this.matchData.teamA_score) {
             this.matchData.status = 'team_B_wins'; // B has higher (less negative) score
        } else {
             this.matchData.status = 'draw_pending_resolution'; // Scores are equal (e.g., both 0 or both -5)
        }
         console.log(`Both teams <= 0. A: ${this.matchData.teamA_score}, B: ${this.matchData.teamB_score}. Status: ${this.matchData.status}`);
    } else if (aDead) {
        this.matchData.status = 'team_B_wins';
         console.log(`Team A <= 0. Status: ${this.matchData.status}`);
    } else if (bDead) {
        this.matchData.status = 'team_A_wins';
         console.log(`Team B <= 0. Status: ${this.matchData.status}`);
    } else {
        this.matchData.status = 'round_finished'; // Neither team is dead
         console.log(`Neither team <= 0. Status: ${this.matchData.status}`);
    }
    console.log(`Final Health - A: ${this.matchData.teamA_score}, B: ${this.matchData.teamB_score}. New Status: ${this.matchData.status}`);

    // Prepare round summary (for logging/debugging)
    const roundSummary = {
        round: this.matchData.round,
        initial_A_Health_this_round: currentAHealth, // Health at the start of this round's calculation
        initial_B_Health_this_round: currentBHealth, // Health at the start of this round's calculation
        teamA_percentage: teamAPercentage,
        teamB_percentage: teamBPercentage,
        teamA_player: this.matchData.teamA_player,
        teamB_player: this.matchData.teamB_player,
        teamA_profession: teamACurrentProfession,
        teamB_profession: teamBCurrentProfession,
        teamA_base_damage_digits: teamADamageDigits,
        teamB_base_damage_digits: teamBDamageDigits,
        teamA_modified_damage_to_B: teamAModifiedDamageToB,
        teamB_modified_damage_to_A: teamBModifiedDamageToA,
        health_after_direct_damage_A: healthAfterDamageA,
        health_after_direct_damage_B: healthAfterDamageB,
        raw_overflow_damage_to_A_by_B: rawOverflowDamageToA,
        raw_overflow_damage_to_B_by_A: rawOverflowDamageToB,
        teamA_mirror_triggered_this_turn: teamAMirrorUsedThisTurn,
        teamB_mirror_triggered_this_turn: teamBMirrorUsedThisTurn,
        simultaneous_mirror_trigger: (canAInitiallyTriggerMirror && canBInitiallyTriggerMirror),
        teamA_reflected_by_defender_mirror: teamAReflectedDamageByDefenderMirror,
        teamB_reflected_by_defender_mirror: teamBReflectedDamageByDefenderMirror,
        teamA_attacker_mirror_extra_damage: teamAAttackerMirrorExtraDamage,
        teamB_attacker_mirror_extra_damage: teamBAttackerMirrorExtraDamage,
        teamA_supporter_base_skill_heal: teamAHealFromSupporterSkill,
        teamB_supporter_base_skill_heal: teamBHealFromSupporterSkill,
        teamA_supporter_mirror_bonus_heal: teamAHealFromSupporterMirrorBonus,
        teamB_supporter_mirror_bonus_heal: teamBHealFromSupporterMirrorBonus,
        final_A_health_before_rounding: finalHealthA, // Log pre-rounded health
        final_B_health_before_rounding: finalHealthB, // Log pre-rounded health
        final_A_health: this.matchData.teamA_score,
        final_B_health: this.matchData.teamB_score,
        new_status: this.matchData.status,
        teamA_mirror_available_after: this.matchData.teamA_mirror_available,
        teamB_mirror_available_after: this.matchData.teamB_mirror_available,
    };
    console.log("Round Summary:", JSON.stringify(roundSummary, null, 2));

    // --- 7. Save State and Broadcast ---
    try {
        await this.state.storage.put('matchData', this.matchData);
        this.broadcast(this.matchData);

        // If match ended, close websockets
        if (['team_A_wins', 'team_B_wins', 'draw_pending_resolution', 'archived_in_d1'].includes(this.matchData.status)) {
             this.websockets.forEach(ws => ws.close(1000, `Match ended. Status: ${this.matchData.status}`));
             this.websockets = [];
        }
        return { success: true, message: `Round ${this.matchData.round} calculated. New status: ${this.matchData.status}`, roundSummary };
    } catch (e: any) {
        console.error(`DO (${this.matchId}) Failed to save state after calculation:`, e);
        return { success: false, message: `Failed to save state after calculation: ${e.message}` };
    }
  }


  // Archive the current round's data to D1 round_archives table
  private async archiveCurrentRound(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to archive round." };
    }
    if (this.matchData.status === 'archived_in_d1') {
        return { success: false, message: "Match is already archived, cannot archive rounds." };
    }

    // Determine winner for this round's archive based on current scores
    const winnerName = this.determineWinner(this.matchData);

    try {
      const stmt = this.env.DB.prepare(
        `INSERT INTO round_archives (match_do_id, round_number, team_a_name, team_a_score, team_a_player,
                                     team_b_name, team_b_score, team_b_player, status, archived_at, raw_data, winner_team_name)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(match_do_id, round_number) DO UPDATE SET
            team_a_name = excluded.team_a_name,
            team_a_score = excluded.team_a_score,
            team_a_player = excluded.team_a_player,
            team_b_name = excluded.team_b_name,
            team_b_score = excluded.team_b_score,
            team_b_player = excluded.team_b_player,
            status = excluded.status,
            archived_at = excluded.archived_at,
            raw_data = excluded.raw_data,
            winner_team_name = excluded.winner_team_name`
      );

      const result = await stmt.bind(
        this.matchData.matchId,
        this.matchData.round,
        this.matchData.teamA_name,
        this.matchData.teamA_score,
        this.matchData.teamA_player, // Archive the player name for this round
        this.matchData.teamB_name,
        this.matchData.teamB_score,
        this.matchData.teamB_player, // Archive the player name for this round
        this.matchData.status,
        new Date().toISOString(),
        JSON.stringify(this.matchData), // Store the full state snapshot
        winnerName
      ).run();

      if (result.success) {
        console.log(`DO (${this.matchId}) Round ${this.matchData.round} data archived/updated in D1 round_archives.`);
        return { success: true, message: `Round ${this.matchData.round} archived.`, d1RecordId: result.meta.last_row_id };
      } else {
        console.error(`DO (${this.matchId}) failed to archive round ${this.matchData.round} to D1:`, result.error);
        return { success: false, message: `Failed to archive round: ${result.error}` };
      }

    } catch (e: any) {
      console.error(`DO (${this.matchId}) exception during D1 round archive:`, e);
      return { success: false, message: `Exception during round archive: ${e.message}` };
    }
  }

  // Advance to the next round
  private async nextRound(): Promise<{ success: boolean; message?: string }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to advance round." };
    }
     if (this.matchData.status === 'archived_in_d1') {
        return { success: false, message: "Match is already archived, cannot advance round." };
    }
     if (this.matchData.status !== 'round_finished') {
        return { success: false, message: `Match status is '${this.matchData.status}'. Must be 'round_finished' to advance.` };
     }

    // Automatically archive the current round before advancing
    const archiveResult = await this.archiveCurrentRound();
    if (!archiveResult.success) {
        console.warn(`DO (${this.matchId}) Failed to auto-archive current round ${this.matchData.round} before advancing:`, archiveResult.message);
        // Decide if you want to stop here or proceed anyway
        // For now, we proceed but log the warning.
    }

    this.matchData.round += 1;
    // Scores are NOT reset between rounds, they carry over.

    // Advance players based on current order
    const teamAOrderLength = this.matchData.teamA_player_order_ids.length;
    const teamBOrderLength = this.matchData.teamB_player_order_ids.length;

    if (teamAOrderLength === 0 || teamBOrderLength === 0) {
         const msg = "Player order list is empty. Cannot advance round.";
         console.error(`DO (${this.matchId}): ${msg}`);
         return { success: false, message: msg };
    }

    this.matchData.current_player_index_a = (this.matchData.current_player_index_a + 1) % teamAOrderLength;
    this.matchData.current_player_index_b = (this.matchData.current_player_index_b + 1) % teamBOrderLength;

    // Update current player names and professions based on the new index
    const currentMemberA = this.getMemberById(this.matchData.teamA_player_order_ids[this.matchData.current_player_index_a], this.matchData.teamA_members);
    const currentMemberB = this.getMemberById(this.matchData.teamB_player_order_ids[this.matchData.current_player_index_b], this.matchData.teamB_members);

    this.matchData.teamA_player = currentMemberA?.nickname || '未知选手';
    this.matchData.teamB_player = currentMemberB?.nickname || '未知选手';
    this.matchData.teamA_current_player_profession = this.getInternalProfession(currentMemberA?.job);
    this.matchData.teamB_current_player_profession = this.getInternalProfession(currentMemberB?.job);


    this.matchData.status = 'pending'; // Reset status to pending for the new round

    try {
      await this.state.storage.put('matchData', this.matchData);
      this.broadcast(this.matchData);
      console.log(`DO (${this.matchId}) advanced to Round ${this.matchData.round}`);
      return { success: true, message: `Advanced to Round ${this.matchData.round}` };
    } catch (e: any) {
      console.error(`DO (${this.matchId}) failed to advance round:`, e);
      return { success: false, message: `Failed to advance round: ${e.message}` };
    }
  }

  // Archive the entire match summary to D1 matches_archive table
  private async archiveMatch(): Promise<{ success: boolean; message?: string; d1RecordId?: string | number | null }> {
    if (!this.matchData) {
      return { success: false, message: "No match data to archive match." };
    }
    if (this.matchData.status === 'archived_in_d1') { // Prevent re-archiving
        return { success: true, message: "Match already archived.", d1RecordId: this.matchData.matchId };
    }

    // Determine winner for the entire match archive (based on final score)
    const matchWinnerName = this.determineWinner(this.matchData);

    try {
      // Insert/Update into matches_archive
      const archiveStmt = this.env.DB.prepare(
        `INSERT INTO matches_archive (match_do_id, tournament_match_id, final_round, team_a_name, team_a_score, team_a_player,
                                     team_b_name, team_b_score, team_b_player, status, archived_at, raw_data, winner_team_name)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(match_do_id) DO UPDATE SET
            tournament_match_id = excluded.tournament_match_id,
            final_round = excluded.final_round,
            team_a_name = excluded.team_a_name,
            team_a_score = excluded.team_a_score,
            team_a_player = excluded.team_a_player,
            team_b_name = excluded.team_b_name,
            team_b_score = excluded.team_b_score,
            team_b_player = excluded.team_b_player,
            status = excluded.status,
            archived_at = excluded.archived_at,
            raw_data = excluded.raw_data,
            winner_team_name = excluded.winner_team_name`
      );

      const finalMatchState = { ...this.matchData };
      // Ensure the status stored in archive is a final state
      const finalStatusForArchive = ['team_A_wins', 'team_B_wins', 'draw_pending_resolution'].includes(finalMatchState.status)
                          ? finalMatchState.status
                          : 'completed'; // Use 'completed' for matches archived manually before a win/loss state


      const archiveResult = await archiveStmt.bind(
        finalMatchState.matchId,
        finalMatchState.tournamentMatchId, // Bind tournament_match_id
        finalMatchState.round,
        finalMatchState.teamA_name,
        finalMatchState.teamA_score,
        finalMatchState.teamA_player,
        finalMatchState.teamB_name,
        finalMatchState.teamB_score,
        finalMatchState.teamB_player,
        finalStatusForArchive,
        new Date().toISOString(),
        JSON.stringify(finalMatchState), // Store the final state snapshot
        matchWinnerName
      ).run();

      if (!archiveResult.success) {
           console.error(`DO (${this.matchId}) failed to archive match to D1 matches_archive:`, archiveResult.error);
           // Decide if you want to stop here or proceed with updating tournament_matches
           // For now, let's proceed but return failure if archive failed
           return { success: false, message: `Failed to archive match summary: ${archiveResult.error}` };
      }


      // Update the corresponding tournament_matches entry if it exists
      if (this.matchData.tournamentMatchId) {
          try {
              // Need to get the winner's team_id from the teams table based on winner name
              let winnerTeamId: number | null = null;
              if (matchWinnerName) {
                  // Find the team ID based on the archived team name
                  // Note: This assumes team names are unique or you handle potential duplicates
                  const winnerTeam = await this.env.DB.prepare("SELECT id FROM teams WHERE name = ?").bind(matchWinnerName).first<{ id: number }>();
                  if (winnerTeam) {
                      winnerTeamId = winnerTeam.id;
                  } else {
                      console.warn(`DO (${this.matchId}): Could not find team ID for winner name "${matchWinnerName}" to update tournament_matches.`);
                  }
              }

              // Determine the status for the tournament_matches table
              const tournamentMatchStatus = ['team_A_wins', 'team_B_wins'].includes(this.matchData.status) ? 'completed' : 'archived'; // Use 'archived' if it was a draw or manual archive

              const updateTournamentStmt = this.env.DB.prepare(
                  `UPDATE tournament_matches SET
                     status = ?,
                     winner_team_id = ?,
                     match_do_id = ? -- Keep the DO ID linked even after completion
                   WHERE id = ?`
              );
              const updateTournamentResult = await updateTournamentStmt.bind(
                  tournamentMatchStatus, // Mark the scheduled match as completed or archived
                  winnerTeamId, // Bind the winner team ID
                  this.matchData.matchId, // Keep the DO ID link
                  this.matchData.tournamentMatchId
              ).run();

              if (!updateTournamentResult.success) {
                  console.error(`DO (${this.matchId}) failed to update tournament_matches entry ${this.matchData.tournamentMatchId}:`, updateTournamentResult.error);
                  // This is a secondary failure, the match summary is archived, but the schedule isn't updated.
                  // Decide how critical this is. For now, log and continue.
              } else {
                   console.log(`DO (${this.matchId}) updated tournament_matches entry ${this.matchData.tournamentMatchId} status to '${tournamentMatchStatus}'.`);
              }

          } catch (e: any) {
              console.error(`DO (${this.matchId}) exception during tournament_matches update:`, e);
              // Log the exception but don't necessarily fail the whole archive operation if matches_archive succeeded
          }
      }


      // Update DO's internal state to reflect the whole match is archived
      this.matchData.status = 'archived_in_d1'; // Custom status for the DO instance
      await this.state.storage.put('matchData', this.matchData);
      this.broadcast(this.matchData); // Notify clients about the archival status

      // Close WebSockets as the live match is over
      this.websockets.forEach(ws => ws.close(1000, "Match archived and finished."));
      this.websockets = [];

      // Return success based on the matches_archive insertion result
      return { success: true, message: "Match data archived to D1.", d1RecordId: archiveResult.meta.last_row_id || this.matchData.matchId };


    } catch (e: any) {
      console.error(`DO (${this.matchId}) exception during D1 match archive (initial insert):`, e);
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

      console.log(`DO (${this.matchId}) Resolving draw. Winner: ${winnerDesignation}`);

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
          console.error(`DO (${this.matchId}) Failed to auto-archive match after draw resolution:`, archiveResult.message);
          // Decide if you want to revert status or just log error
          // For now, we'll proceed but return the archive error message
          return { success: false, message: `Draw resolved, but failed to archive match: ${archiveResult.message}` };
      }

      console.log(`DO (${this.matchId}) Draw resolved. New status: ${this.matchData.status}`);
      // State is already saved and broadcast by archiveMatch
      return { success: true, message: `Draw resolved. ${this.matchData.status.replace('_', ' ')}.` };
  }


  // Start a new match by resetting the DO state (less preferred, use initializeFromSchedule)
  private async newMatch(): Promise<{ success: boolean; message?: string }> {
     // Only allow starting a new match if the current one is archived
     if (this.matchData?.status !== 'archived_in_d1') {
         return { success: false, message: "Current match must be archived before starting a new one." };
     }

    try {
      // Clear all state stored for this DO instance
      await this.state.storage.deleteAll();
      console.log(`DO (${this.matchId}) storage cleared.`);

      // Initialize with default state for the new match (no associated tournament match)
      this.matchData = { ...defaultMatchState, matchId: this.matchId, tournamentMatchId: null }; // Ensure tournamentMatchId is null
      await this.state.storage.put('matchData', this.matchData);
      console.log(`DO (${this.matchId}) initialized for new match (default).`);

      // Broadcast the new state (clients will see a reset)
      this.broadcast(this.matchData);

      // Note: WebSockets were closed during archiveMatch. Clients will need to reconnect.

      return { success: true, message: "New match started." };
    } catch (e: any) {
      console.error(`DO (${this.matchId}) failed to start new match:`, e);
      return { success: false, message: `Failed to start new match: ${e.message}` };
    }
  }


  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Ensure matchData is loaded (it should be by the constructor's blockConcurrencyWhile)
    // This check is a safeguard, constructor should handle initial load
    // If matchData is still null after constructor, something is wrong.
    if (!this.matchData) {
         console.error(`DO (${this.matchId}): matchData is null after constructor. Critical error.`);
         return new Response(JSON.stringify({ error: "Match data not initialized in DO" }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }


    // WebSocket upgrade
    if (url.pathname === '/websocket') { // Internal path for WS
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
      }
      const [client, server] = Object.values(new WebSocketPair());
      this.websockets.push(server);
      server.accept();
      console.log(`DO (${this.matchId}) WebSocket connected. Total: ${this.websockets.length}`);

      // Send current state immediately upon connection
      server.send(JSON.stringify(this.matchData));

      // Handle messages from this specific client (optional, e.g., for pings)
      server.addEventListener('message', event => {
        console.log(`DO (${this.matchId}) WS message from client:`, event.data);
        // server.send(`Echo: ${event.data}`); // Example echo
      });
      server.addEventListener('close', (event) => {
        console.log(`DO (${this.matchId}) WebSocket closed. Code: ${event.code}, Reason: ${event.reason}`);
        this.websockets = this.websockets.filter(ws => ws.readyState === WebSocket.OPEN);
        console.log(`DO (${this.matchId}) WebSocket disconnected. Remaining: ${this.websockets.length}`);
      });
      server.addEventListener('error', (err) => {
        console.error(`DO (${this.matchId}) WebSocket error:`, err);
        this.websockets = this.websockets.filter(ws => ws !== server);
         console.log(`DO (${this.matchId}) WebSocket error disconnected. Remaining: ${this.websockets.length}`);
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    // Get current state
    if (url.pathname === '/state' && request.method === 'GET') { // Internal path
      return new Response(JSON.stringify(this.matchData), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // --- Internal Endpoints for Actions ---

    // Internal endpoint to initialize DO state from schedule data
    if (url.pathname === '/internal/initialize-from-schedule' && request.method === 'POST') {
        try {
            const scheduleData = await request.json<MatchScheduleData>();
             if (!scheduleData || scheduleData.tournamentMatchId === undefined || !scheduleData.team1_name || !scheduleData.team2_name || !Array.isArray(scheduleData.team1_members) || !Array.isArray(scheduleData.team2_members) || !Array.isArray(scheduleData.team1_player_order_ids) || !Array.isArray(scheduleData.team2_player_order_ids)) {
                 return new Response(JSON.stringify({ error: "Invalid schedule data payload" }), { status: 400, headers: { 'Content-Type': 'application/json' } });
             }

            const initResult = await this.initializeFromSchedule(scheduleData);
            if (initResult.success) {
                return new Response(JSON.stringify(initResult), { headers: { 'Content-Type': 'application/json' } });
            } else {
                return new Response(JSON.stringify(initResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
            }
        } catch (e: any) {
             console.error(`DO (${this.matchId}) Exception processing initialize-from-schedule payload:`, e);
             return new Response(JSON.stringify({ error: 'Invalid initialize-from-schedule payload', details: e.message }), { status: 400 });
        }
    }

    // Internal endpoint to calculate round outcome
     if (url.pathname === '/internal/calculate-round' && request.method === 'POST') {
         try {
             const payload = await request.json<CalculateRoundPayload>();
             if (typeof payload.teamA_percentage !== 'number' || typeof payload.teamB_percentage !== 'number') {
                 return new Response(JSON.stringify({ error: "Invalid calculate-round payload: percentages must be numbers." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
             }
             const result = await this.calculateRoundOutcome(payload);
             if (result.success) {
                 return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } });
             } else {
                 // Return 400 if it's a state-related error, 500 for internal calculation/save errors
                 const status = result.message?.includes("Cannot calculate round") ? 400 : 500;
                 return new Response(JSON.stringify(result), { status: status, headers: { 'Content-Type': 'application/json' } });
             }
         } catch (e: any) {
             console.error(`DO (${this.matchId}) Exception processing calculate-round payload:`, e);
             return new Response(JSON.stringify({ error: 'Invalid calculate-round payload', details: e.message }), { status: 400 });
         }
     }


    // Internal endpoint to archive current round data to D1
    // Note: This is primarily called automatically by nextRound, but exposed for manual trigger if needed.
    if (url.pathname === '/internal/archive-round' && request.method === 'POST') {
      const archiveResult = await this.archiveCurrentRound();
      if (archiveResult.success) {
        return new Response(JSON.stringify(archiveResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(archiveResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

    // Internal endpoint to advance to the next round
    if (url.pathname === '/internal/next-round' && request.method === 'POST') {
       const nextRoundResult = await this.nextRound();
       if (nextRoundResult.success) {
           return new Response(JSON.stringify(nextRoundResult), { headers: { 'Content-Type': 'application/json' } });
       } else {
           // Return 400 if it's a state-related error, 500 for internal errors
           const status = nextRoundResult.message?.includes("Cannot advance round") ? 400 : 500;
           return new Response(JSON.stringify(nextRoundResult), { status: status, headers: { 'Content-Type': 'application/json' } });
       }
    }

    // Internal endpoint to archive the entire match to D1
    if (url.pathname === '/internal/archive-match' && request.method === 'POST') {
      const archiveResult = await this.archiveMatch();
      if (archiveResult.success) {
        return new Response(JSON.stringify(archiveResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(archiveResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }

     // Internal endpoint to resolve a draw
    if (url.pathname === '/internal/resolve-draw' && request.method === 'POST') {
        try {
            const payload = await request.json<ResolveDrawPayload>();
            if (payload.winner !== 'teamA' && payload.winner !== 'teamB') {
                 return new Response(JSON.stringify({ error: "Invalid resolve-draw payload: winner must be 'teamA' or 'teamB'." }), { status: 400, headers: { 'Content-Type': 'application/json' } });
            }
            const result = await this.resolveDraw(payload.winner);
            if (result.success) {
                 return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } });
            } else {
                 // Return 400 if it's a state-related error, 500 for internal errors
                 const status = result.message?.includes("Must be 'draw_pending_resolution'") ? 400 : 500;
                 return new Response(JSON.stringify(result), { status: status, headers: { 'Content-Type': 'application/json' } });
            }
        } catch (e: any) {
             console.error(`DO (${this.matchId}) Exception processing resolve-draw payload:`, e);
             return new Response(JSON.stringify({ error: 'Invalid resolve-draw payload', details: e.message }), { status: 400 });
        }
    }


     // Internal endpoint to start a new match (default, unscheduled) - Use initializeFromSchedule instead
     // Keeping this as a potential admin backdoor or for testing unscheduled matches
    if (url.pathname === '/internal/new-match' && request.method === 'POST') {
      const newMatchResult = await this.newMatch();
      if (newMatchResult.success) {
        return new Response(JSON.stringify(newMatchResult), { headers: { 'Content-Type': 'application/json' } });
      } else {
        return new Response(JSON.stringify(newMatchResult), { status: 500, headers: { 'Content-Type': 'application/json' } });
      }
    }


    // Fallback for unknown internal paths
    return new Response('Durable Object: Not found or method not allowed for this path', { status: 404 });
  }
}
