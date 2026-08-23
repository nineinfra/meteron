import { reactive } from 'vue'

// 会话状态（API Key / 客户筛选 / 时间范围），持久化到 localStorage
export const session = reactive({
  key: localStorage.getItem('meteron_key') || '',
  customer: localStorage.getItem('meteron_customer') || '',
  hours: Number(localStorage.getItem('meteron_hours') || 24),
})

export function persistSession() {
  localStorage.setItem('meteron_key', session.key)
  localStorage.setItem('meteron_customer', session.customer)
  localStorage.setItem('meteron_hours', String(session.hours))
}

export function clearSession() {
  localStorage.removeItem('meteron_key')
  session.key = ''
}

export class UnauthorizedError extends Error {
  constructor(msg = 'unauthorized') { super(msg) }
}

// api 统一封装：Bearer 鉴权 + {code,msg,data} 信封解包
export async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: {
      Authorization: 'Bearer ' + session.key,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  })
  let body = {}
  try { body = await res.json() } catch { /* 非 JSON 响应 */ }
  if (res.status === 401) throw new UnauthorizedError(body.msg || 'unauthorized')
  if (!res.ok || body.code !== 0) throw new Error(body.msg || ('HTTP ' + res.status))
  return body.data
}

/* ---------- 格式化 ---------- */

export function fmtNum(n) {
  if (n == null) return '-'
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return String(n)
}

export function fmtAmount(s, currency) {
  if (s == null) return '-'
  const n = Number(s)
  const v = Number.isInteger(n) ? n.toFixed(2) : n.toFixed(6).replace(/0+$/, '').replace(/\.$/, '')
  return (currency || 'USD') + ' ' + v
}

export function fmtPeriod(start, end) {
  const f = (iso) => new Date(iso).toISOString().slice(0, 10)
  return f(start) + ' ~ ' + f(end)
}

export function fmtTime(iso) {
  if (!iso) return '-'
  return new Date(iso).toLocaleString('zh-CN', { hour12: false })
}
