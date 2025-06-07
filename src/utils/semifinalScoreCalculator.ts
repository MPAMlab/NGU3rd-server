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
  originalScore: number; // 基础得分（四位数字映射 0->10 后的总和）
  bonusScore: number; // 职业技能加成
  totalScore: number; // 最终得分
  log: string[]; // 计分日志
}

/**
 * 从完成率中提取小数点后四位数字 (0-9)。
 * @param percentage 完成率，如 99.9876
 * @returns 包含四个数字 (0-9) 的数组，或在失败时返回 null。
 */
function extractFourDigits(percentage: number): number[] | null {
  // 确保输入是有效有限数字
  if (typeof percentage !== 'number' || !isFinite(percentage)) {
      console.error("Invalid percentage input to extractFourDigits:", percentage);
      return null;
  }

  // 使用 toFixed(4) 转换为精确到小数点后四位的字符串
  const percentageString = percentage.toFixed(4); // 例如: "99.9876", "0.0001", "100.0000"

  const parts = percentageString.split('.');

  let decimalPart = '';
  if (parts.length === 2) {
      // 正常情况，有整数部分和小数部分
      decimalPart = parts[1]; // 获取小数点后的部分
  } else {
       // 理论上 toFixed(4) 总是会产生小数点和四位小数，除非输入本身是 NaN/Infinity
       // 如果 split('.') 结果不是两部分，说明格式异常
       console.error(`Unexpected percentageString format in extractFourDigits: ${percentageString}`);
       return null;
  }

  // 确保我们取到的是 4 位数字，padEnd 和 substring 增加健壮性
  const paddedDecimalPart = decimalPart.padEnd(4, '0').substring(0, 4); // 确保是4位

  const digits: number[] = [];
  for (let i = 0; i < 4; i++) {
      const digitChar = paddedDecimalPart[i];
      const digit = parseInt(digitChar, 10);
      // 检查是否成功解析为数字
      if (isNaN(digit)) {
          console.error(`Failed to parse digit "${digitChar}" at index ${i} from "${paddedDecimalPart}".`);
          return null; // 如果任何一个字符不是数字，则失败
      }
      digits.push(digit);
  }

  // console.log(`extractFourDigits(${percentage}): percentageString=${percentageString}, digits=${digits}`);

  return digits; // 返回包含四个 0-9 数字的数组
}

/**
 * 将 0-9 的数字映射到 1-10 的伤害值。
 * 0 -> 10, 1-9 -> 自身
 * @param digit 0-9 的数字
 * @returns 1-10 的伤害值
 */
function mapDigitToScoreValue(digit: number): number {
    if (digit === 0) {
        return 10;
    } else if (digit >= 1 && digit <= 9) {
        return digit;
    } else {
        // 理论上 extractFourDigits 应该只返回 0-9 的数字，这里做个防御性检查
        console.error("Invalid digit input to mapDigitToScoreValue:", digit);
        return 0; // 返回 0 或其他默认值表示无效
    }
}

/**
 * 计算复赛得分
 * @param player 当前计算得分的选手数据
 * @param opponent 对手选手数据
 * @returns 计分结果 SemifinalScoreResult
 */
export function calculateSemifinalScore(player: PlayerCalculationData, opponent: PlayerCalculationData): SemifinalScoreResult {
  const log: string[] = [];

  log.push(`--- 开始计算 ${player.nickname} (${player.profession}) 的得分 ---`);
  log.push(`${player.nickname} 完成率: ${player.percentage.toFixed(4)}%`);
  log.push(`${opponent.nickname} 完成率: ${opponent.percentage.toFixed(4)}%`);


  // 提取自身和对手的四位数字 (0-9)
  const playerDigits = extractFourDigits(player.percentage);
  const opponentDigits = extractFourDigits(opponent.percentage);

  // 检查数字提取是否成功
  if (!playerDigits || playerDigits.length !== 4) {
      log.push(`错误：无法从 ${player.nickname} 完成率提取有效的四位数字。`);
      console.error(`calculateSemifinalScore: Failed to extract valid digits for ${player.nickname} from percentage ${player.percentage}`);
      return {
          id: player.id,
          nickname: player.nickname,
          profession: player.profession,
          originalScore: NaN,
          bonusScore: NaN,
          totalScore: NaN,
          log: log
      };
  }
   if (!opponentDigits || opponentDigits.length !== 4) {
       log.push(`警告：无法从对手 ${opponent.nickname} 完成率提取有效的四位数字，依赖对手的职业加成将为 0。`);
       console.warn(`calculateSemifinalScore: Failed to extract valid opponent digits for ${opponent.nickname} from percentage ${opponent.percentage}. Bonus will be 0.`);
   }


  // 计算基础得分 (四位数字映射 0->10 后的总和)
  const playerBaseScoreValues = playerDigits.map(mapDigitToScoreValue);
  const playerBaseScore = playerBaseScoreValues.reduce((sum, value) => sum + value, 0);
  log.push(`${player.nickname} 四位数字 (0-9): [${playerDigits.join(', ')}]`);
  log.push(`${player.nickname} 映射得分值 (0->10): [${playerBaseScoreValues.join(', ')}]`);
  log.push(`${player.nickname} 基础得分 (总和): ${playerBaseScore}`);


  // 计算职业特性加成
  let bonusScore = 0;
  let professionBonusLog = '';

  if (player.profession === '绝剑士') {
      // 绝剑士：额外获得一次得分，数值等同于自身基础得分数字（映射后 1-10）中最高的一位
      const highestScoreValue = Math.max(...playerBaseScoreValues); // 找到映射后值中的最高位
      bonusScore = highestScoreValue;
      professionBonusLog = `绝剑士特性：额外获得 ${bonusScore} 分，来自自身得分值 [${playerBaseScoreValues.join(', ')}] 中的最高位。`;
      log.push(professionBonusLog);

  } else if (player.profession === '矩盾手') {
      // 矩盾手：额外获得一次得分，数值等同于对手基础得分数字（映射后 1-10）中随机一位
      if (opponentDigits && opponentDigits.length === 4) { // 只有对手数字有效时才计算加成
          const opponentScoreValues = opponentDigits.map(mapDigitToScoreValue);
          const randomIndex = Math.floor(Math.random() * 4); // 0-3之间的随机数
          const randomScoreValue = opponentScoreValues[randomIndex];
          bonusScore = randomScoreValue;
          professionBonusLog = `矩盾手特性：额外获得 ${bonusScore} 分，来自对手 ${opponent.nickname} 得分值 [${opponentScoreValues.join(', ')}] 的第 ${randomIndex + 1} 位数字。`;
          log.push(professionBonusLog);
      } else {
           professionBonusLog = `警告：对手 ${opponent.nickname} 数字无效，矩盾手加成为 0。`;
           log.push(professionBonusLog);
      }

  } else if (player.profession === '炼星师') {
      // 炼星师：额外获得一次得分，数值等同于对手基础得分数字（映射后 1-10）中个位数（即第四位数字）
      // "如果双方都是炼星师则计算对方无被动技能的原分数的个位数" - 这个规则在单次函数调用中难以判断“对方无被动技能的原分数”
      // 且“个位数”根据例子 (3+8+5+9=26, 个位数6) 似乎是指总和的个位数，而不是第四位数字。
      // 让我们按照例子来：炼星师加成 = (对手基础得分总和) 的个位数。
      // 如果双方都是炼星师，则加成基于对方的“基础得分总和”（即四位数字映射 0->10 后的总和），取这个总和的个位数。
      // 这个“基础得分总和”就是我们上面计算的 opponentBaseScore。
      if (opponentDigits && opponentDigits.length === 4) { // 只有对手数字有效时才计算加成
          const opponentBaseScoreValues = opponentDigits.map(mapDigitToScoreValue);
          const opponentBaseScoreSum = opponentBaseScoreValues.reduce((sum, value) => sum + value, 0);
          const lastDigitOfSum = opponentBaseScoreSum % 10; // 取总和的个位数
          bonusScore = lastDigitOfSum;
          professionBonusLog = `炼星师特性：额外获得 ${bonusScore} 分，来自对手 ${opponent.nickname} 基础得分总和 (${opponentBaseScoreSum}) 的个位数。`;
          log.push(professionBonusLog);
      } else {
          professionBonusLog = `警告：对手 ${opponent.nickname} 数字无效，炼星师加成为 0。`;
          log.push(professionBonusLog);
      }

  } else {
      professionBonusLog = `未知职业 "${player.profession}"，无职业加成。`;
      log.push(professionBonusLog);
      console.warn(`calculateSemifinalScore: Unknown profession "${player.profession}" for ${player.nickname}. No bonus applied.`);
  }

  // 计算总分
  const totalScore = playerBaseScore + bonusScore;
  log.push(`最终得分：基础得分 ${playerBaseScore} + 职业加成 ${bonusScore} = ${totalScore}`);


  return {
      id: player.id,
      nickname: player.nickname,
      profession: player.profession,
      originalScore: playerBaseScore, // 基础得分是四位数字映射 0->10 后的总和
      bonusScore: bonusScore,
      totalScore: totalScore,
      log: log
  };
}
