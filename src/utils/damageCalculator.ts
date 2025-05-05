import { DamageCalculationInput, DamageCalculationResult, Member } from '../types';

// Helper to extract last 4 digits from score percentage string
function extractDamageDigits(scorePercent: string): number[] {
  const parts = scorePercent.split('.');
  if (parts.length < 2) {
    // Handle scores like "100" - maybe treat as 0000? Or error?
    // Based on example "100.4902", assume format is always X.YYYY
    console.warn(`Score string "${scorePercent}" does not contain a decimal point.`);
    return [0, 0, 0, 0]; // Or throw an error
  }
  const decimalPart = parts[1];
  const digits: number[] = [];
  for (let i = 0; i < 4; i++) {
    if (i < decimalPart.length) {
      const digit = parseInt(decimalPart[i], 10);
      digits.push(isNaN(digit) ? 0 : digit); // Treat non-numeric as 0
    } else {
      digits.push(0); // Pad with 0 if less than 4 digits
    }
  }
  return digits;
}

// Helper to sum an array of numbers
function sum(numbers: number[]): number {
  return numbers.reduce((acc, curr) => acc + curr, 0);
}

// Helper for 矩盾手 random negation index
function getRandomInt(max: number): number {
    // Returns a random integer from 0 to max-1
    return Math.floor(Math.random() * max);
}


export function calculateMatchTurnResult(input: DamageCalculationInput): DamageCalculationResult {
  let currentTeam1Health = input.team1Health;
  let currentTeam2Health = input.team2Health;
  let team1HasMirror = input.team1HasMirror;
  let team2HasMirror = input.team2HasMirror;

  const log: string[] = [];
  log.push(`--- Turn Start ---`);
  log.push(`Initial Health: Team 1 = ${currentTeam1Health}, Team 2 = ${currentTeam2Health}`);
  log.push(`Mirrors: Team 1 = ${team1HasMirror ? 'Yes' : 'No'}, Team 2 = ${team2HasMirror ? 'Yes' : 'No'}`);
  log.push(`Scores: Team 1 = ${input.scorePercent1}, Team 2 = ${input.scorePercent2}`);
  log.push(`Professions: Team 1 = ${input.team1Profession}, Team 2 = ${input.team2Profession}`);

  // 1. Extract Damage Digits
  const team1Digits = extractDamageDigits(input.scorePercent1);
  const team2Digits = extractDamageDigits(input.scorePercent2);
  log.push(`Team 1 Digits: [${team1Digits.join(', ')}]`);
  log.push(`Team 2 Digits: [${team2Digits.join(', ')}]`);

  // 2. Calculate Base Damage
  const team1BaseDamage = sum(team1Digits);
  const team2BaseDamage = sum(team2Digits);
  log.push(`Team 1 Base Damage: ${team1BaseDamage}`);
  log.push(`Team 2 Base Damage: ${team2BaseDamage}`);

  // Initialize skill/mirror effect variables
  let team1PotentialBonus = 0; // 绝剑士 bonus
  let team2PotentialBonus = 0;
  let team1PotentialHeal = 0; // 炼星师 heal/negation
  let team2PotentialHeal = 0;
  let team1Negation = 0; // 矩盾手 negation
  let team2Negation = 0;
  let team1MirrorUsedThisTurn = false;
  let team2MirrorUsedThisTurn = false;
  let team1MirrorExtraDamage = 0; // Damage dealt by mirror effect (矩盾手 reflect)
  let team2MirrorExtraDamage = 0;
  let team1MirrorExtraHeal = 0; // Extra heal from mirror effect (炼星师 double)
  let team2MirrorExtraHeal = 0;

  // 3. Calculate Potential Skill Effects (based on own profession)
  if (input.team1Profession === '绝剑士') {
    team1PotentialBonus = Math.max(...team1Digits);
    log.push(`Team 1 (绝剑士) potential bonus damage: ${team1PotentialBonus}`);
  }
  if (input.team2Profession === '绝剑士') {
    team2PotentialBonus = Math.max(...team2Digits);
    log.push(`Team 2 (绝剑士) potential bonus damage: ${team2PotentialBonus}`);
  }
  if (input.team1Profession === '炼星师') {
    team1PotentialHeal = Math.max(...team1Digits);
    log.push(`Team 1 (炼星师) potential heal/negation: ${team1PotentialHeal}`);
  }
  if (input.team2Profession === '炼星师') {
    team2PotentialHeal = Math.max(...team2Digits);
    log.push(`Team 2 (炼星师) potential heal/negation: ${team2PotentialHeal}`);
  }

  // 4. Calculate Damage Dealt (before opponent's negation)
  const team1DamageDealt = team1BaseDamage + team1PotentialBonus;
  const team2DamageDealt = team2BaseDamage + team2PotentialBonus;
  log.push(`Team 1 Damage Dealt (before opponent negation): ${team1DamageDealt}`);
  log.push(`Team 2 Damage Dealt (before opponent negation): ${team2DamageDealt}`);


  // 5. Calculate Damage Negation (based on opponent's 矩盾手)
  if (input.team1Profession === '矩盾手') {
      // Opponent is Team 2. Damage sources are team2Digits and team2PotentialBonus.
      const opponentDamageSources = [...team2Digits];
      if (team2PotentialBonus > 0) {
          opponentDamageSources.push(team2PotentialBonus); // Add 绝剑士 bonus as a potential target
      }
      if (opponentDamageSources.length > 0) {
          const randomIndex = getRandomInt(opponentDamageSources.length);
          team1Negation = opponentDamageSources[randomIndex];
          log.push(`Team 1 (矩盾手) negates Team 2 damage source at index ${randomIndex}: ${team1Negation}`);
      } else {
           log.push(`Team 1 (矩盾手) opponent had no damage sources to negate.`);
      }
  }
   if (input.team2Profession === '矩盾手') {
      // Opponent is Team 1. Damage sources are team1Digits and team1PotentialBonus.
      const opponentDamageSources = [...team1Digits];
      if (team1PotentialBonus > 0) {
          opponentDamageSources.push(team1PotentialBonus); // Add 绝剑士 bonus as a potential target
      }
       if (opponentDamageSources.length > 0) {
            const randomIndex = getRandomInt(opponentDamageSources.length);
            team2Negation = opponentDamageSources[randomIndex];
            log.push(`Team 2 (矩盾手) negates Team 1 damage source at index ${randomIndex}: ${team2Negation}`);
       } else {
           log.push(`Team 2 (矩盾手) opponent had no damage sources to negate.`);
       }
  }


  // 6. Calculate Damage Taken (after opponent's negation and own 炼星师 negation)
  // 炼星师 negates their *own* highest damage segment from the damage they *deal*
  // The MATLAB code effectively does this by adding the heal amount back to their health *before* taking damage.
  // Let's calculate damage taken as opponent's dealt damage minus negation.
  // The 炼星师 heal will be applied later.
  const team1DamageTaken = Math.max(0, team2DamageDealt - team1Negation); // Damage Team 1 takes
  const team2DamageTaken = Math.max(0, team1DamageDealt - team2Negation); // Damage Team 2 takes
  log.push(`Team 1 Damage Taken (after negation): ${team1DamageTaken}`);
  log.push(`Team 2 Damage Taken (after negation): ${team2DamageTaken}`);


  // 7. Apply Damage Taken
  currentTeam1Health -= team1DamageTaken;
  currentTeam2Health -= team2DamageTaken;
  log.push(`Health after applying damage: Team 1 = ${currentTeam1Health}, Team 2 = ${currentTeam2Health}`);

  // 8. Check and Apply Mirror Effects (Sequential A then B)
  // Note: Mirror effects (like 矩盾手 reflect or 绝剑士 bonus) are applied *after* the initial damage
  // and *after* the mirror heals to 20.

  // Store health before mirror checks for calculating overflow
  const healthBeforeMirrorCheck1 = currentTeam1Health;
  const healthBeforeMirrorCheck2 = currentTeam2Health;

  // Team 1 Mirror Check
  if (currentTeam1Health <= 0 && team1HasMirror) {
    team1MirrorUsedThisTurn = true;
    team1HasMirror = false; // Mirror is consumed

    log.push(`Team 1 Mirror Triggered!`);

    // Calculate overflow damage from THIS hit for 矩盾手 reflection
    const team1Overflow = Math.abs(healthBeforeMirrorCheck1);
    log.push(`Team 1 overflow damage from this hit: ${team1Overflow}`);

    // Apply Team 1 Mirror Profession Effect
    if (input.team1Profession === '绝剑士') {
       // MATLAB adds max damage again. Let's follow that.
       // Note: The text says "额外追加一段死仇". MATLAB adds max(digits).
       // Let's add max(digits) again as per MATLAB.
       const extraDamage = Math.max(...team1Digits);
       team2MirrorExtraDamage += extraDamage; // Damage dealt TO Team 2
       log.push(`Team 1 (绝剑士) mirror adds extra damage to Team 2: ${extraDamage}`);
    } else if (input.team1Profession === '矩盾手') {
       // Reflect overflow damage
       team2MirrorExtraDamage += team1Overflow; // Damage dealt TO Team 2
       log.push(`Team 1 (矩盾手) mirror reflects overflow damage to Team 2: ${team1Overflow}`);
    } else if (input.team1Profession === '炼星师') {
       // Double the heal amount
       team1MirrorExtraHeal += team1PotentialHeal; // Extra heal FOR Team 1
       log.push(`Team 1 (炼星师) mirror doubles heal amount: ${team1PotentialHeal}`);
    }

    // Heal Team 1 to 20 AFTER calculating overflow but BEFORE applying opponent's mirror damage
    currentTeam1Health = 20;
    log.push(`Team 1 health restored to 20.`);
  }


  // Team 2 Mirror Check (based on health *after* Team 1's potential mirror effects)
  if (currentTeam2Health <= 0 && team2HasMirror) {
    team2MirrorUsedThisTurn = true;
    team2HasMirror = false; // Mirror is consumed

    log.push(`Team 2 Mirror Triggered!`);

    // Calculate overflow damage from THIS hit for 矩盾手 reflection
    const team2Overflow = Math.abs(healthBeforeMirrorCheck2);
     log.push(`Team 2 overflow damage from this hit: ${team2Overflow}`);

    // Apply Team 2 Mirror Profession Effect
    if (input.team2Profession === '绝剑士') {
       const extraDamage = Math.max(...team2Digits);
       team1MirrorExtraDamage += extraDamage; // Damage dealt TO Team 1
       log.push(`Team 2 (绝剑士) mirror adds extra damage to Team 1: ${extraDamage}`);
    } else if (input.team2Profession === '矩盾手') {
       // Reflect overflow damage
       team1MirrorExtraDamage += team2Overflow; // Damage dealt TO Team 1
       log.push(`Team 2 (矩盾手) mirror reflects overflow damage to Team 1: ${team2Overflow}`);
    } else if (input.team2Profession === '炼星师') {
       // Double the heal amount
       team2MirrorExtraHeal += team2PotentialHeal; // Extra heal FOR Team 2
       log.push(`Team 2 (炼星师) mirror doubles heal amount: ${team2PotentialHeal}`);
    }

    // Heal Team 2 to 20 AFTER calculating overflow but BEFORE applying opponent's mirror damage
    currentTeam2Health = 20;
    log.push(`Team 2 health restored to 20.`);
  }

  // 9. Apply Mirror Extra Damage (from opponent's mirror effect)
  if (team1MirrorExtraDamage > 0) {
      currentTeam1Health -= team1MirrorExtraDamage;
      log.push(`Team 1 takes extra damage from Team 2 mirror effect: ${team1MirrorExtraDamage}`);
  }
   if (team2MirrorExtraDamage > 0) {
      currentTeam2Health -= team2MirrorExtraDamage;
      log.push(`Team 2 takes extra damage from Team 1 mirror effect: ${team2MirrorExtraDamage}`);
  }
   log.push(`Health after applying mirror extra damage: Team 1 = ${currentTeam1Health}, Team 2 = ${currentTeam2Health}`);


  // 10. Apply Healing (炼星师)
  const team1TotalHeal = team1PotentialHeal + team1MirrorExtraHeal;
  const team2TotalHeal = team2PotentialHeal + team2MirrorExtraHeal;

  if (team1TotalHeal > 0) {
      currentTeam1Health += team1TotalHeal;
      log.push(`Team 1 heals: ${team1TotalHeal}`);
  }
  if (team2TotalHeal > 0) {
      currentTeam2Health += team2TotalHeal;
      log.push(`Team 2 heals: ${team2TotalHeal}`);
  }
  log.push(`Health after applying healing: Team 1 = ${currentTeam1Health}, Team 2 = ${currentTeam2Health}`);


  // 11. Calculate Health Changes
  const team1HealthChange = currentTeam1Health - input.team1Health;
  const team2HealthChange = currentTeam2Health - input.team2Health;
  log.push(`Net Health Change: Team 1 = ${team1HealthChange}, Team 2 = ${team2HealthChange}`);


  log.push(`--- Turn End ---`);

  return {
    team1HealthAfter: currentTeam1Health,
    team2HealthAfter: currentTeam2Health,
    team1MirrorUsedThisTurn: team1MirrorUsedThisTurn,
    team2MirrorUsedThisTurn: team2MirrorUsedThisTurn,
    team1DamageDealt: team1DamageDealt, // Damage calculated from score + skills before negation
    team2DamageDealt: team2DamageDealt, // Damage calculated from score + skills before negation
    team1DamageTaken: team1DamageTaken, // Damage taken after opponent's negation (before own heal)
    team2DamageTaken: team2DamageTaken, // Damage taken after opponent's negation (before own heal)
    team1HealthChange: team1HealthChange,
    team2HealthChange: team2HealthChange,
    log: log,
  };
}
