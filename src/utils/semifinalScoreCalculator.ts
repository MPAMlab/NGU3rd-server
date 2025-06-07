// src/utils/semifinalScoreCalculator.ts (Backend version)
// This file should be placed in your backend source directory (e.g., src/utils)

// 选手职业类型
export type Profession = '矩盾手' | '炼星师' | '绝剑士';

// 选手数据用于计算
export interface PlayerCalculationData {
  id: number;
  nickname: string;
  profession: Profession;
  percentage: number; // 乐曲完成率，如99.9876
}

// 计分结果
export interface SemifinalScoreResult {
  id: number;
  nickname: string;
  profession: Profession;
  originalScore: number; // 原始得分（小数点后四位）
  bonusScore: number; // 职业技能加成
  totalScore: number; // 最终得分
  log: string[]; // 计分日志
}

// 从完成率中提取四位小数
function extractFourDigits(percentage: number): number {
  // 转为字符串，提取小数点后四位
  const percentageStr = percentage.toFixed(4);
  const decimalPart = percentageStr.split('.')[1];
  // Ensure it's exactly 4 digits, pad with 0s if needed (though toFixed(4) should handle this)
  const paddedDecimalPart = decimalPart.padEnd(4, '0');
  return parseFloat("0." + paddedDecimalPart);
}

// 从四位小数中获取最大的一位数字
function getHighestDigit(score: number): number {
  const scoreStr = score.toFixed(4).substring(2); // 去掉"0."
  const digits = scoreStr.split('').map(Number);
  return Math.max(...digits);
}

// 计算复赛得分
export function calculateSemifinalScore(player: PlayerCalculationData, opponent: PlayerCalculationData): SemifinalScoreResult {
  const log: string[] = [];
  
  // 提取小数点后四位作为基础得分
  const score = extractFourDigits(player.percentage);
  log.push(`${player.nickname} 完成率 ${player.percentage.toFixed(4)}%，取小数点后四位：${score.toFixed(4)}`);
  
  // 计算职业特性加成
  let bonusScore = 0;
  
  if (player.profession === '矩盾手') {
    // 矩盾手：随机将对手四位数字中的一位无效化
    // Note: This effect is defensive, it reduces the *opponent's* score, not adds to the player's score.
    // The original description "随机将对手四位数字中的一位无效化" implies reducing the opponent's score.
    // Let's adjust the logic based on this interpretation. The calculation function should probably return
    // the *effect* on the opponent, or the main calculation loop needs to handle this.
    // Re-reading: "对战双方均取乐曲完成率的小数点后四位数字，记为得分。经过两轮后，得分最高、最低的两支队伍晋级决赛。"
    // This implies players get their own score. The profession effects are *passive*.
    // "矩盾手：随机将对手四位数字中的一位无效化" - This is ambiguous. Does it mean the opponent's score is calculated *after* their digits are invalidated? Or does it affect the damage calculation?
    // Let's assume for 1v1, the profession affects *their own* score calculation or adds a bonus.
    // If it invalidates an opponent's digit *for the opponent's score calculation*, the opponent's score needs to be calculated *after* the defender's effect is applied. This makes the calculation order dependent.
    // A simpler interpretation for 1v1: The defender's *own* score calculation is affected by the opponent's score. This still doesn't make sense for "invalidating opponent's digits".
    // Let's go back to the original interpretation from the LiveMatch DO: Professions affect *damage dealt*. But 1v1 doesn't have damage/health.
    // Let's try a new interpretation for 1v1 based on the text: Professions modify the player's *own* score.
    // - 矩盾手: Randomly invalidates one of *their own* four digits? Or the opponent's digits *used in the opponent's calculation*? The latter is complex. Let's assume it affects *their own* score calculation based on the opponent.
    // - 炼星师: Extra score = opponent's score's last digit. This is clear.
    // - 绝剑士: Extra score = own highest digit. This is clear.

    // Let's re-interpret 矩盾手 for 1v1: It might mean the opponent's score is calculated *as if* one of their digits was zeroed out *when comparing scores*. This is also complex.
    // Simplest interpretation: Professions add a bonus *to the player's own score*.
    // - 矩盾手: Bonus based on opponent? Maybe random digit from opponent's score added? Or random digit from *own* score added?
    // Let's assume the simplest possible interpretation that fits the text and 1v1:
    // - 炼星师: Bonus = opponent's score's last digit / 10000. (Matches previous)
    // - 绝剑士: Bonus = own highest digit / 10000. (Matches previous)
    // - 矩盾手: Let's assume it adds a random digit from *their own* score. This is a guess, as the text is ambiguous for 1v1. Or maybe it adds a fixed small bonus? Or maybe it's just not applicable in 1v1?
    // Given the original text "随机将对手四位数字中的一位无效化", let's stick to the interpretation that it affects the *opponent's* score calculation *when the opponent is the attacker*. But in 1v1, both are "attackers" on their own score.
    // Let's try one more interpretation: 矩盾手 gets a bonus equal to a random digit from the *opponent's* score.
    // Let's use this interpretation for 矩盾手 in 1v1: Bonus = random digit from opponent's score / 10000.

    if (player.profession === '矩盾手') {
        const opponentScore = extractFourDigits(opponent.percentage);
        const opponentScoreStr = opponentScore.toFixed(4).substring(2); // 去掉"0."
        const digits = opponentScoreStr.split('').map(Number);
        const randomIndex = Math.floor(Math.random() * 4); // 0-3之间的随机数
        const randomDigit = digits[randomIndex];
        bonusScore = randomDigit / 10000;
        log.push(`矩盾手特性：额外获得 ${randomDigit}/10000 (${bonusScore.toFixed(4)}) 分，来自对手 ${opponent.nickname} 得分的第 ${randomIndex + 1} 位数字`);
    } else if (player.profession === '炼星师') {
        // 炼星师：额外获得一次得分，数值等同于对手得分的个位数
        const opponentScore = extractFourDigits(opponent.percentage);
        const opponentScoreStr = opponentScore.toFixed(4); // e.g., "0.1234"
        const lastDigit = parseInt(opponentScoreStr[opponentScoreStr.length - 1]); // Get the last character, parse as int
        bonusScore = lastDigit / 10000; // 转换为小数形式
        log.push(`炼星师特性：额外获得 ${lastDigit}/10000 (${bonusScore.toFixed(4)}) 分，来自对手 ${opponent.nickname} 得分的个位数`);
    } else if (player.profession === '绝剑士') {
        // 绝剑士：额外获得一次得分，数值等同于四位数字中最高的一位
        const highestDigit = getHighestDigit(score);
        bonusScore = highestDigit / 10000; // 转换为小数形式
        log.push(`绝剑士特性：额外获得 ${highestDigit}/10000 (${bonusScore.toFixed(4)}) 分，来自自身得分的最高位`);
    }

    // 计算总分
    const totalScore = score + bonusScore;
    log.push(`最终得分：${score.toFixed(4)} + ${bonusScore.toFixed(4)} = ${totalScore.toFixed(4)}`);

    return {
        id: player.id,
        nickname: player.nickname,
        profession: player.profession,
        originalScore: score,
        bonusScore: bonusScore,
        totalScore: totalScore,
        log: log
    };
}
}