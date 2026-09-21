<script setup lang="ts">
import type { AccountRow } from '../constants'
import type { AutoWakeRecord } from '@/api/modules/auto-wake'
import { ref, watch } from 'vue'
import { getAccountModels } from '@/api'
import { getAutoWake, saveAutoWake } from '@/api/modules/auto-wake'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { errorMessage } from '@/utils/async'
import AccountIdentityCell from './AccountIdentityCell.vue'

const props = defineProps<{ account: AccountRow | null }>()
const open = defineModel<boolean>({ required: true })
const record = ref<AutoWakeRecord | null>(null)
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const models = ref<{ label: string, value: string }[]>([])
const triggers = [
  { label: '定时', value: 'scheduled' },
  { label: '5 小时额度重置后', value: 'fiveHourReset' },
  { label: '周额度重置后', value: 'weeklyReset' },
  { label: '任一额度重置后', value: 'eitherReset' },
]
let generation = 0

watch(() => [open.value, props.account?.id], async () => {
  const current = ++generation
  record.value = null
  error.value = ''
  if (!open.value || !props.account)
    return
  loading.value = true
  try {
    const id = props.account.id
    const [settings, catalog] = await Promise.allSettled([getAutoWake(id), getAccountModels({ accountId: id }, { silent: true })])
    if (generation !== current)
      return
    if (settings.status === 'rejected')
      throw settings.reason
    record.value = settings.value
    models.value = catalog.status === 'fulfilled' ? catalog.value.models.map(model => ({ label: model.id, value: model.id })) : []
    if (catalog.status === 'rejected')
      error.value = '模型目录暂不可用，仍可关闭计划并保存'
  }
  catch (cause) {
    if (generation === current)
      error.value = errorMessage(cause, '读取唤醒设置失败')
  }
  finally {
    if (generation === current)
      loading.value = false
  }
})

async function save() {
  if (!record.value || saving.value)
    return
  saving.value = true
  error.value = ''
  try {
    record.value = await saveAutoWake(record.value.accountId, record.value.config)
    toast.success('自动唤醒设置已保存')
  }
  catch (cause) {
    error.value = errorMessage(cause, '保存失败')
  }
  finally {
    saving.value = false
  }
}

function time(value: number | null) {
  return value ? new Date(value * 1000).toLocaleString() : '—'
}
</script>

<template>
  <BaseModal v-model="open" title="自动唤醒" size="md-wide" :dismissible="!saving">
    <div class="grid gap-4">
      <AccountIdentityCell v-if="account" :account="account" size="lg" />
      <p v-if="loading" role="status" class="text-cp-sm text-cp-text-secondary">
        正在读取设置…
      </p>
      <p v-if="error" role="alert" class="text-cp-sm text-cp-error">
        {{ error }}
      </p>
      <template v-if="record">
        <div class="flex items-center justify-between gap-3">
          <span class="text-cp text-cp-text">启用自动唤醒</span>
          <BaseSwitch v-model="record.config.enabled" label="启用自动唤醒" :disabled="saving" />
        </div>
        <p class="m-0 text-cp-sm text-cp-text-secondary">
          向此账号发送最小推理请求，会消耗少量额度，每次至少间隔 5 分钟
        </p>
        <BaseFormItem label="触发方式">
          <BaseSelect v-model="record.config.trigger" :options="triggers" :disabled="saving" />
        </BaseFormItem>
        <BaseFormItem label="唤醒模型">
          <BaseSelect v-model="record.config.model" :options="models" :disabled="saving" placeholder="选择上游模型" />
        </BaseFormItem>
        <template v-if="record.config.trigger === 'scheduled'">
          <BaseFormItem label="Cron" description="分 时 日 月 周，例如 0 8,13,18 * * *">
            <BaseInput v-model="record.config.cron" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="时区" description="IANA 时区，例如 Asia/Shanghai 或 Europe/Berlin">
            <BaseInput v-model="record.config.timezone" :disabled="saving" />
          </BaseFormItem>
        </template>
        <p v-else class="m-0 text-cp-sm text-cp-text-secondary">
          跟踪账号公共额度窗口，重置后等待至少 2 分钟再复核，额度查询失败时继续等待
        </p>
        <dl class="m-0 grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
          <dt class="text-cp-text-secondary">
            下次定时
          </dt>
          <dd class="m-0 text-cp-text">
            {{ time(record.state.nextRunAt) }}
          </dd>
          <dt class="text-cp-text-secondary">
            上次执行
          </dt>
          <dd class="m-0 text-cp-text">
            {{ time(record.state.lastAttemptAt) }}
          </dd>
          <dt class="text-cp-text-secondary">
            最近结果
          </dt>
          <dd class="m-0 text-cp-text">
            {{ record.state.lastMessage || '尚未执行' }}
          </dd>
        </dl>
      </template>
    </div>
    <template #footer>
      <BaseButton :disabled="saving" @click="open = false">
        关闭
      </BaseButton>
      <BaseButton variant="primary" :loading="saving" :disabled="loading || !record" @click="save">
        保存
      </BaseButton>
    </template>
  </BaseModal>
</template>
