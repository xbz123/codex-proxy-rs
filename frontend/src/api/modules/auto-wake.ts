import request from '../request'

export interface AutoWakeConfig {
  enabled: boolean
  trigger: 'scheduled' | 'fiveHourReset' | 'weeklyReset' | 'eitherReset'
  model: string
  cron: string
  timezone: string
}

export interface AutoWakeRecord {
  accountId: string
  generation: number
  config: AutoWakeConfig
  state: {
    nextRunAt: number | null
    lastAttemptAt: number | null
    lastStatus: string | null
    lastMessage: string | null
  }
}

export function getAutoWake(accountId: string) {
  return request<AutoWakeRecord>({
    url: '/api/admin/accounts/auto-wake',
    method: 'GET',
    params: { accountId },
  })
}

export function saveAutoWake(accountId: string, config: AutoWakeConfig) {
  return request<AutoWakeRecord>({
    url: '/api/admin/accounts/auto-wake',
    method: 'POST',
    data: { accountId, config },
  })
}
