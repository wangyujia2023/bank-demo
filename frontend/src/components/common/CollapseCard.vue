<template>
  <div class="card collapse-card">
    <div class="collapse-hd" @click="emit('update:open', !open)">
      <slot name="header-left" />
      <span class="ch-tog">{{ open ? '▲' : '▼' }}</span>
    </div>
    <div v-show="open" class="arch-body">
      <slot />
    </div>
  </div>
</template>

<script setup>
defineProps({
  open: { type: Boolean, default: false },
})

const emit = defineEmits(['update:open'])
</script>
