export function normalizePairCodeInput(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const exactGroup = raw.match(/(?:^|\D)(\d{6})(?:\D|$)/);
  if (exactGroup?.[1]) return exactGroup[1];
  const digits = raw.replace(/\D+/g, '');
  return digits.slice(0, 6);
}
