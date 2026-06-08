const $ = (id) => document.getElementById(id);
let activeRunId = null;
let polling = false;
let refreshPaused = false;
let tableReady = false;

function fixed(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function ensureRunsTable() {
  if (tableReady) return;
  $("runs").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Run</th>
          <th>Mode</th>
          <th>Status</th>
          <th>Time</th>
          <th>Cong.</th>
          <th>Avg FCT</th>
          <th>Flow BW</th>
          <th>Util</th>
          <th>Wall</th>
          <th>Trace</th>
          <th>Schedule</th>
          <th>Log</th>
          <th>Delete</th>
        </tr>
      </thead>
      <tbody id="runsBody"></tbody>
    </table>
  `;
  tableReady = true;
}

function logText(run) {
  return run.error ? `ERROR: ${run.error}\n\n${run.log_tail || ""}` : run.log_tail || "";
}

function schedulerProgress(run) {
  return run.scheduler_progress || {};
}

function rangeText(summary) {
  if (!summary || !summary.count) return "none";
  const sample = (summary.sample || [])
    .slice(0, 3)
    .map((range) => `${range.start}-${range.end}`)
    .join(", ");
  return `${summary.count} ranges, ${summary.total_length} ticks${sample ? ` (${sample})` : ""}`;
}

function timingText(timings) {
  if (!timings || !timings.length) return "-";
  return timings.slice(0, 4).map((timing) => {
    const rates = (timing.throttle_rates || []).join("/");
    const deltas = (timing.delta_sample || []).slice(0, 3).join(",");
    return `J${timing.job_id}: delta sum ${timing.delta_sum}, max ${timing.delta_max}, rates ${rates || "-"}, sample [${deltas}]`;
  }).join("; ");
}

function pillList(items) {
  return (items || []).map((item) => `<span class="algorithm-pill">${escapeHtml(item)}</span>`).join("");
}

function patternMemberText(pattern) {
  const members = pattern.members || [];
  if (!members.length) {
    const sources = (pattern.src_racks || []).join(",");
    const destinations = (pattern.dst_racks || []).join(",");
    return sources || destinations ? `racks ${sources} -> ${destinations}` : "";
  }

  return members.map((member) => {
    const flows = member.flow_count > 1 ? `, ${member.flow_count} flows` : "";
    return `J${member.job_id} R${member.src_rack}->R${member.dst_rack} x${member.parallel_edge_count}${flows}`;
  }).join("; ");
}

function eventInfo(event) {
  if (event.phase === "routing" && event.status === "started") {
    const flows = event.flows || {};
    const jobs = (flows.jobs || []).map((item) => `J${item.job_id}:${item.flow_count}`);
    const pairs = (flows.rack_pairs || []).slice(0, 4).map((item) => `${item.src_rack}->${item.dst_rack}:${item.flow_count}`);
    return `strategy ${event.strategy}, ${flows.flow_count || 0} flows, t=${flows.time_start ?? "-"}-${flows.time_end ?? "-"} ${pillList(jobs)} ${pillList(pairs)}`;
  }
  if (event.phase === "routing" && event.status === "coloring_precheck") {
    return `lower bound ${fixed(event.max_required_spines_lower_bound, 2)} spines, available ${event.available_spines}, subflow cap ${fixed(event.subflow_capacity, 2)}`;
  }
  if (event.phase === "routing" && event.status === "traffic_patterns_built") {
    const patterns = (event.pattern_sample || []).slice(0, 3).map((item) => {
      const members = patternMemberText(item);
      return `${item.pattern}: ${rangeText(item.time_ranges)}${members ? `; ${members}` : ""}`;
    });
    return `${event.traffic_pattern_count} traffic patterns ${pillList(patterns)}`;
  }
  if (event.phase === "routing" && event.status === "coloring_solved") {
    const groups = (event.group_sample || []).slice(0, 3).map((item) => {
      const verdict = item.fits ? "fits" : "over";
      return `${fixed(item.used_spines, 2)}/${event.available_spines ?? "?"} spines, ${item.flow_count} flows, ${verdict}`;
    });
    return `${event.merged_group_count} merged groups, bad ${rangeText(event.bad_ranges)} ${pillList(groups)}`;
  }
  if (event.phase === "routing" && event.status === "decisions_ready") {
    const sample = (event.decision_sample || []).slice(0, 3).map((item) => {
      const spines = (item.spine_rates || []).map(([spine, share]) => `s${spine}:${fixed(share, 2)}`).join("/");
      return `J${item.job_id} f${item.flow_id} i${item.iteration} ${spines}`;
    });
    return `${event.decision_count} routing decisions, bad ${rangeText(event.bad_ranges)} ${pillList(sample)}`;
  }
  if (event.phase === "timing" && event.status === "timing_produced") {
    return timingText(event.timings);
  }
  if (event.phase === "timing" && event.status === "round_evaluated") {
    const remaining = event.remaining_bad_ranges || event.bad_ranges;
    return `remaining ${rangeText(remaining)}, fixed ${rangeText(event.fixed_bad_ranges)}, ratios rem=${fixed(event.remaining_bad_range_ratio, 3)} fixed=${fixed(event.fixed_bad_range_ratio, 3)}`;
  }
  if (event.phase === "timing" && event.status === "round_started") {
    return `${event.reason || ""} ${event.fixed_bad_ranges ? `fixing ${rangeText(event.fixed_bad_ranges)}` : ""}`;
  }
  if (event.phase === "scheduling" && event.status === "artifacts_written") {
    return `${event.timed_jobs} timed jobs, ${event.routing_decisions} routing decisions`;
  }
  return event.job_id ? `job ${event.job_id}` : event.routing_decisions ? `${event.routing_decisions} routes` : "";
}

function renderTimingRounds(progress) {
  const rounds = progress.timing_rounds || [];
  if (!rounds.length) return `<tr><td colspan="5" class="subtle">No timing rounds yet.</td></tr>`;
  return rounds.map((round) => {
    const evaluated = round.evaluated || {};
    return `
      <tr>
        <td>${escapeHtml(round.round)}</td>
        <td>${escapeHtml(round.step || "-")}</td>
        <td class="detail-cell">${escapeHtml(timingText(round.timings))}</td>
        <td class="detail-cell">${escapeHtml(rangeText(evaluated.remaining_bad_ranges || evaluated.bad_ranges))}</td>
        <td>${escapeHtml(evaluated.routing_decisions ?? "-")}</td>
      </tr>
    `;
  }).join("");
}

function eventRoundLabel(event) {
  if (event.phase === "timing" && event.round !== undefined) {
    return {
      key: `round-${event.round}`,
      label: `Round ${event.round}`,
      detail: event.step ? `${event.step} timing` : "timing"
    };
  }

  if (event.phase === "routing" && event.suffix !== undefined) {
    const suffix = String(event.suffix);
    const parts = suffix.split("_");
    const round = parts.length > 1 ? parts[parts.length - 1] : suffix;
    const detail = parts.length > 1 ? `routing suffix ${suffix}` : "routing";
    return {
      key: `round-${round}`,
      label: `Round ${round}`,
      detail
    };
  }

  return {
    key: `phase-${event.phase || "other"}`,
    label: event.phase ? `${event.phase[0].toUpperCase()}${event.phase.slice(1)}` : "Other",
    detail: "outside timing rounds"
  };
}

function pct(value, max) {
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0) return 0;
  return Math.max(0, Math.min(100, (value / max) * 100));
}

function roundGroups(progress) {
  const groups = [];
  const byKey = new Map();

  for (const event of progress.algorithm_events || []) {
    const group = eventRoundLabel(event);
    if (!group.key.startsWith("round-")) continue;
    if (!byKey.has(group.key)) {
      const item = { ...group, events: [] };
      byKey.set(group.key, item);
      groups.push(item);
    }
    byKey.get(group.key).events.push(event);
  }

  return groups;
}

function findRoundEvent(round, phase, status) {
  return round.events.find((event) => event.phase === phase && event.status === status) || {};
}

function profilePeriods(profiles) {
  const periods = new Map();
  for (const profile of profiles || []) {
    periods.set(`${profile.job_id}:${profile.throttle}`, Number(profile.period));
  }
  return periods;
}

function timingIntervals(timing, periods) {
  if (timing.intervals?.length) return timing.intervals;

  let cursor = 0;
  return (timing.delta_sample || []).map((delta, iteration) => {
    const throttle = timing.throttle_sample?.[iteration] ?? 1;
    const period = periods.get(`${timing.job_id}:${throttle}`) || 0;
    const delayStart = cursor;
    const delayEnd = delayStart + delta;
    const periodStart = delayEnd;
    const periodEnd = periodStart + period;
    cursor = periodEnd;
    return {
      iteration,
      delay_start: delayStart,
      delay_end: delayEnd,
      period_start: periodStart,
      period_end: periodEnd,
      period,
      throttle
    };
  });
}

function renderTimingVisual(event, profiles) {
  const timings = event.timings || [];
  if (!timings.length) return `<div class="subtle">-</div>`;
  const periods = profilePeriods(profiles);
  const jobs = timings.slice(0, 5).map((timing) => ({
    timing,
    intervals: timingIntervals(timing, periods)
  }));
  const timelineEnd = Math.max(
    1,
    ...jobs.flatMap(({ intervals }) => intervals.map((interval) => interval.period_end))
  );

  return jobs.map(({ timing, intervals }) => {
    const segments = intervals.map((interval) => {
      const delayLength = interval.delay_end - interval.delay_start;
      const delay = delayLength > 0
        ? `<span class="timing-delay" title="Iteration ${interval.iteration} delay: ${interval.delay_start}-${interval.delay_end}" style="left:${pct(interval.delay_start, timelineEnd)}%;width:${pct(delayLength, timelineEnd)}%"></span>`
        : "";
      const activeLength = interval.period_end - interval.period_start;
      const active = activeLength > 0
        ? `<span class="timing-period" title="Iteration ${interval.iteration}: ${interval.period_start}-${interval.period_end}, throttle ${interval.throttle}" style="left:${pct(interval.period_start, timelineEnd)}%;width:${pct(activeLength, timelineEnd)}%"></span>`
        : "";
      return `${delay}${active}`;
    }).join("");
    const end = intervals.at(-1)?.period_end ?? "-";
    return `
      <div class="job-line visual-list-item">
        <span>J${escapeHtml(timing.job_id)}</span>
        <span class="bar-track timing-track">${segments}</span>
        <span>t=${escapeHtml(end)}</span>
      </div>
    `;
  }).join("");
}

function renderTrafficVisual(event) {
  const patterns = event.pattern_sample || [];
  if (!patterns.length) return `<div class="subtle">-</div>`;
  const maxLength = Math.max(1, ...patterns.map((pattern) => pattern.time_ranges?.total_length || 0));

  return patterns.slice(0, 5).map((pattern, index) => {
    const length = pattern.time_ranges?.total_length || 0;
    const encodedPattern = encodeURIComponent(JSON.stringify(pattern));
    return `
      <div class="pattern-entry visual-list-item">
        <div class="pattern-line">
          <button
            class="pattern-trigger ${index === 0 ? "active" : ""}"
            type="button"
            data-pattern="${escapeHtml(encodedPattern)}"
          >${escapeHtml(String(pattern.pattern || "").slice(0, 6))}</button>
          <span class="bar-track"><span class="pattern-bar" style="width:${pct(length, maxLength)}%"></span></span>
          <span>${escapeHtml(pattern.time_ranges?.count ?? "-")}x</span>
        </div>
      </div>
    `;
  }).join("");
}

function jobEdgeColor(jobId) {
  const colors = ["#1c3934", "#ba6a36", "#c3955b", "#795d54", "#8f3150", "#39708a"];
  return colors[Math.abs(Number(jobId) || 0) % colors.length];
}

function renderPatternGraph(pattern) {
  const members = pattern?.members || [];
  if (!members.length) {
    return `
      <div class="pattern-graph-title">
        <span>${escapeHtml(String(pattern?.pattern || "Pattern").slice(0, 8))}</span>
      </div>
      <div class="subtle">Edge membership unavailable for this run.</div>
    `;
  }

  const srcRacks = [...new Set(members.map((member) => Number(member.src_rack)))].sort((a, b) => a - b);
  const dstRacks = [...new Set(members.map((member) => Number(member.dst_rack)))].sort((a, b) => a - b);
  const width = 280;
  const height = 140;
  const leftX = 42;
  const rightX = width - 42;
  const top = 28;
  const bottom = height - 16;
  const nodeY = (rack, racks) => {
    const index = racks.indexOf(rack);
    if (racks.length === 1) return (top + bottom) / 2;
    return top + (index / (racks.length - 1)) * (bottom - top);
  };
  const edgeStrands = members.flatMap((member) => {
    const count = Math.max(1, Number(member.parallel_edge_count) || 1);
    return Array.from({ length: count }, (_, strand) => ({
      ...member,
      strand,
      strand_count: count
    }));
  });
  const pairTotals = new Map();
  const pairSeen = new Map();

  for (const edge of edgeStrands) {
    const key = `${edge.src_rack}:${edge.dst_rack}`;
    pairTotals.set(key, (pairTotals.get(key) || 0) + 1);
  }

  const edges = edgeStrands.map((edge) => {
    const key = `${edge.src_rack}:${edge.dst_rack}`;
    const edgeIndex = pairSeen.get(key) || 0;
    pairSeen.set(key, edgeIndex + 1);
    const edgeCount = pairTotals.get(key);
    const lane = edgeCount === 1 ? 0 : (edgeIndex / (edgeCount - 1)) * 2 - 1;
    const y1 = nodeY(Number(edge.src_rack), srcRacks);
    const y2 = nodeY(Number(edge.dst_rack), dstRacks);
    const endpointOffset = lane * 6;
    const curveOffset = lane * Math.min(42, 12 + edgeCount * 2.5);
    const color = jobEdgeColor(edge.job_id);
    const description = `Job ${edge.job_id}: rack ${edge.src_rack} to rack ${edge.dst_rack}, edge ${edge.strand + 1} of ${edge.strand_count}`;
    return `
      <path
        class="graph-edge"
        d="M ${leftX + 9} ${y1 + endpointOffset}
           C ${leftX + 70} ${y1 + curveOffset},
             ${rightX - 70} ${y2 + curveOffset},
             ${rightX - 9} ${y2 + endpointOffset}"
        stroke="${color}"
        stroke-width="1.5"
      ><title>${escapeHtml(description)}</title></path>
    `;
  }).join("");
  const sourceNodes = srcRacks.map((rack) => {
    const y = nodeY(rack, srcRacks);
    return `<circle class="graph-node" cx="${leftX}" cy="${y}" r="9"></circle><text class="graph-node-label" x="${leftX}" y="${y}">R${rack}</text>`;
  }).join("");
  const destinationNodes = dstRacks.map((rack) => {
    const y = nodeY(rack, dstRacks);
    return `<circle class="graph-node" cx="${rightX}" cy="${y}" r="9"></circle><text class="graph-node-label" x="${rightX}" y="${y}">R${rack}</text>`;
  }).join("");
  const legend = members.map((member) => {
    const color = jobEdgeColor(member.job_id);
    return `
      <span class="edge-key">
        <span class="edge-swatch" style="--edge-color:${color}"></span>
        J${escapeHtml(member.job_id)} R${escapeHtml(member.src_rack)}-&gt;R${escapeHtml(member.dst_rack)} x${escapeHtml(member.parallel_edge_count)}
      </span>
    `;
  }).join("");
  const patternCount = pattern.patterns?.length;
  const graphSummary = patternCount
    ? `${patternCount} patterns, ${pattern.parallel_edge_count ?? "-"} edges`
    : `${pattern.parallel_edge_count ?? "-"} edges`;
  const graphLegend = patternCount
    ? ""
    : `<div class="pattern-graph-legend">${legend}</div>`;

  return `
    <div class="pattern-graph-title">
      <span>${escapeHtml(String(pattern.pattern || "").slice(0, 8))}</span>
      <span>${escapeHtml(graphSummary)}</span>
    </div>
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Bipartite rack graph for pattern ${escapeHtml(pattern.pattern)}">
      <text class="graph-side-label" x="${leftX}" y="12" text-anchor="middle">source</text>
      <text class="graph-side-label" x="${rightX}" y="12" text-anchor="middle">destination</text>
      ${edges}
      ${sourceNodes}
      ${destinationNodes}
    </svg>
    ${graphLegend}
  `;
}

function patternGraphData(trigger) {
  try {
    return JSON.parse(decodeURIComponent(trigger.dataset.pattern));
  } catch {
    return null;
  }
}

function showPatternGraph(trigger) {
  const row = trigger.closest(".visual-row");
  const graph = row?.querySelector(".pattern-graph");
  const pattern = patternGraphData(trigger);
  if (!graph || !pattern) return;

  for (const item of row.querySelectorAll(".pattern-trigger")) {
    item.classList.toggle("active", item === trigger);
  }
  const associatedPatterns = new Set(pattern.patterns || []);
  for (const item of row.querySelectorAll(".pattern-entry .pattern-trigger")) {
    const itemPattern = patternGraphData(item);
    item.classList.toggle("associated", associatedPatterns.has(itemPattern?.pattern));
  }
  graph.innerHTML = renderPatternGraph(pattern);
}

function mergePatternMembers(group, trafficEvent) {
  if (group.members?.length) return group.members;

  const patternsById = new Map(
    (trafficEvent.pattern_sample || []).map((pattern) => [pattern.pattern, pattern])
  );
  const patternIds = group.patterns || [];
  if (!patternIds.length || !patternIds.every((patternId) => patternsById.get(patternId)?.members?.length)) {
    return [];
  }
  const memberCounts = new Map();

  for (const patternId of patternIds) {
    const pattern = patternsById.get(patternId);
    for (const member of pattern.members) {
      const key = `${member.job_id}:${member.src_rack}:${member.dst_rack}:${member.needed_subflows}`;
      const current = memberCounts.get(key) || {
        job_id: member.job_id,
        src_rack: member.src_rack,
        dst_rack: member.dst_rack,
        needed_subflows: member.needed_subflows,
        flow_count: 0,
        parallel_edge_count: 0
      };
      current.flow_count += Number(member.flow_count) || 0;
      current.parallel_edge_count += Number(member.parallel_edge_count) || 0;
      memberCounts.set(key, current);
    }
  }

  return [...memberCounts.values()];
}

function mergedPatternPayload(group, trafficEvent, index) {
  const members = mergePatternMembers(group, trafficEvent);
  return {
    pattern: `Merged ${index + 1}`,
    patterns: group.patterns || [],
    members,
    parallel_edge_count: group.parallel_edge_count
      ?? members.reduce((sum, member) => sum + (Number(member.parallel_edge_count) || 0), 0),
    time_ranges: group.ranges
  };
}

function renderColoringVisual(event, trafficEvent) {
  const groups = event.group_sample || [];
  if (!groups.length) return `<div class="subtle">-</div>`;
  const available = event.available_spines || 1;

  return groups.slice(0, 5).map((group, index) => {
    const used = group.used_spines || 0;
    const barClass = group.fits ? "spine-bar" : "spine-bar over";
    const mergedPattern = mergedPatternPayload(group, trafficEvent, index);
    const encodedPattern = encodeURIComponent(JSON.stringify(mergedPattern));
    const patternNames = (group.patterns || [])
      .map((pattern) => String(pattern).slice(0, 6))
      .join(" + ");
    return `
      <div class="merged-pattern-entry visual-list-item">
        <div class="coloring-line">
          <button
            class="pattern-trigger merged-pattern-trigger"
            type="button"
            data-pattern="${escapeHtml(encodedPattern)}"
            title="${escapeHtml(`Merged from ${patternNames || "one pattern"}`)}"
          >M${index + 1}</button>
          <span class="bar-track"><span class="${barClass}" style="width:${pct(used, available)}%"></span></span>
          <span>${fixed(used, 1)}/${escapeHtml(available)}</span>
        </div>
        <div class="merged-pattern-members" title="${escapeHtml(patternNames)}">
          ${escapeHtml(patternNames || "single pattern")}
        </div>
      </div>
    `;
  }).join("");
}

function renderBadTimeline(summary, start, end) {
  if (!summary || !summary.count || !Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return `<span class="bar-track bad-timeline"></span>`;
  }

  const ranges = (summary.sample || []).slice(0, 5).map((range) => {
    const left = pct(range.start - start, end - start);
    const width = Math.max(2, pct(range.end - range.start + 1, end - start));
    return `<span class="bad-range" style="left:${left}%;width:${width}%"></span>`;
  }).join("");
  return `<span class="bar-track bad-timeline">${ranges}</span>`;
}

function renderOutcomeVisual(evaluated, routingStarted, strategyFinished) {
  if (!evaluated.status) return `<div class="subtle">-</div>`;
  const ratio = Number(evaluated.remaining_bad_range_ratio ?? 0);
  const fill = pct(Math.min(ratio, 1), 1);
  const ranges = evaluated.remaining_bad_ranges || evaluated.bad_ranges;
  return `
    <div class="bar-track"><span class="bad-fill" style="width:${fill}%"></span></div>
    ${renderBadTimeline(ranges, routingStarted.flows?.time_start, strategyFinished.affected_time_end)}
    <div class="visual-note">${fixed(ratio, 3)} remaining, ${escapeHtml(evaluated.routing_decisions ?? "-")} routes</div>
  `;
}

function renderSchedulerVisual(progress) {
  const groups = roundGroups(progress);
  if (!groups.length) return `<div class="subtle">No round visualization yet.</div>`;

  const rows = groups.map((round) => {
    const roundStarted = findRoundEvent(round, "timing", "round_started");
    const timingProduced = findRoundEvent(round, "timing", "timing_produced");
    const routingStarted = findRoundEvent(round, "routing", "started");
    const trafficPatterns = findRoundEvent(round, "routing", "traffic_patterns_built");
    const coloringSolved = findRoundEvent(round, "routing", "coloring_solved");
    const strategyFinished = findRoundEvent(round, "routing", "strategy_finished");
    const evaluated = findRoundEvent(round, "timing", "round_evaluated");
    const initialPattern = trafficPatterns.pattern_sample?.[0] || null;

    return `
      <div class="visual-row">
        <div class="visual-cell">
          <div class="visual-round">${escapeHtml(round.label)}</div>
          <div class="visual-note">${escapeHtml(roundStarted.step || routingStarted.strategy || "-")}</div>
        </div>
        <div class="visual-cell">${renderTimingVisual(timingProduced, progress.profiles || [])}</div>
        <div class="visual-cell">${renderTrafficVisual(trafficPatterns)}</div>
        <div class="visual-cell pattern-graph">${renderPatternGraph(initialPattern)}</div>
        <div class="visual-cell">${renderColoringVisual(coloringSolved, trafficPatterns)}</div>
        <div class="visual-cell">${renderOutcomeVisual(evaluated, routingStarted, strategyFinished)}</div>
      </div>
    `;
  }).join("");

  return `
    <div class="scheduler-visual">
      <div class="visual-head">
        <div class="visual-cell">Round</div>
        <div class="visual-cell">Timing</div>
        <div class="visual-cell">Patterns</div>
        <div class="visual-cell">Pattern Graph</div>
        <div class="visual-cell">Merged Patterns</div>
        <div class="visual-cell">Result</div>
      </div>
      ${rows}
    </div>
  `;
}

function renderAlgorithmEvents(progress) {
  const events = progress.algorithm_events || [];
  if (!events.length) return `<tr><td colspan="5" class="subtle">No algorithm events yet.</td></tr>`;
  let currentGroup = null;
  const rows = [];

  for (const event of events.slice(-32)) {
    const group = eventRoundLabel(event);
    if (group.key !== currentGroup) {
      rows.push(`
        <tr class="round-group">
          <td colspan="5">${escapeHtml(group.label)}<span class="subtle">${escapeHtml(group.detail)}</span></td>
        </tr>
      `);
      currentGroup = group.key;
    }

    rows.push(`
      <tr>
        <td>${fixed(event.elapsed, 1)}</td>
        <td>${escapeHtml(event.phase || "-")}</td>
        <td>${escapeHtml(event.status || "-")}</td>
        <td>${escapeHtml(event.round ?? event.suffix ?? "-")}</td>
        <td class="detail-cell">${eventInfo(event)}</td>
      </tr>
    `);
  }

  return rows.join("");
}

function renderProfileTable(profiles) {
  if (!profiles.length) {
    return `
      <table class="mini-table">
        <thead><tr><th>Job</th></tr></thead>
        <tbody><tr><td class="subtle">No profile events yet.</td></tr></tbody>
      </table>
    `;
  }

  const rateKey = (value) => String(value);
  const rates = [...new Set(profiles.map((event) => rateKey(event.throttle)))]
    .sort((a, b) => Number(b) - Number(a));
  const jobs = [...new Set(profiles.map((event) => String(event.job_id)))]
    .sort((a, b) => Number(a) - Number(b));
  const byJobAndRate = new Map();

  for (const event of profiles) {
    byJobAndRate.set(`${event.job_id}:${rateKey(event.throttle)}`, event);
  }

  const header = rates.map((rate) => `<th>Rate ${escapeHtml(rate)}</th>`).join("");
  const rows = jobs.map((jobId) => {
    const cells = rates.map((rate) => {
      const event = byJobAndRate.get(`${jobId}:${rate}`);
      if (!event) return `<td class="profile-cell subtle">-</td>`;
      return `
        <td class="profile-cell">
          <span class="profile-period">${fixed(event.period, 0)}</span>
          <span class="profile-flows">${escapeHtml(event.flow_count ?? "-")} flows</span>
        </td>
      `;
    }).join("");
    return `<tr><td>Job ${escapeHtml(jobId)}</td>${cells}</tr>`;
  }).join("");

  return `
    <table class="mini-table">
      <thead><tr><th>Job</th>${header}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderScheduleDetail(run) {
  const progress = schedulerProgress(run);
  const profiles = progress.profiles || [];
  const events = progress.events || [];
  const profileTotal = progress.profile_total || 0;
  const profileFinished = progress.profile_finished || 0;
  const profileText = profileTotal ? `${profileFinished}/${profileTotal}` : "-";
  const elapsedText = Number.isFinite(progress.latest_elapsed) ? `${fixed(progress.latest_elapsed, 1)}s` : "-";
  const eventRows = events.length
    ? events.slice(-8).map((event) => `
      <tr>
        <td>${fixed(event.elapsed, 1)}</td>
        <td>${escapeHtml(event.phase || "-")}</td>
        <td>${escapeHtml(event.status || "-")}</td>
        <td>${escapeHtml(event.job_id ? `job ${event.job_id}` : event.routing_decisions ? `${event.routing_decisions} routes` : "")}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="4" class="subtle">No scheduler events yet.</td></tr>`;

  return `
    <div class="schedule-detail">
      <div><strong>${escapeHtml(progress.latest_phase || "-")}</strong><span class="subtle">phase</span></div>
      <div><strong>${escapeHtml(progress.latest_status || "-")}</strong><span class="subtle">status</span></div>
      <div><strong>${profileText}</strong><span class="subtle">profiles</span></div>
      <div><strong>${escapeHtml(progress.routing_decisions ?? "-")}</strong><span class="subtle">routing decisions</span></div>
      <div><strong>${escapeHtml(progress.fixing_rounds ?? "-")}</strong><span class="subtle">fixing rounds</span></div>
      <div><strong>${fixed(progress.remaining_bad_range_ratio, 3)}</strong><span class="subtle">remaining bad range</span></div>
      <div><strong>${fixed(progress.fixed_bad_range_ratio, 3)}</strong><span class="subtle">fixed bad range</span></div>
      <div><strong>${elapsedText}</strong><span class="subtle">scheduler elapsed</span></div>
      <div class="full">
        ${renderSchedulerVisual(progress)}
      </div>
      <div class="full">
        ${renderProfileTable(profiles)}
      </div>
      <div class="full">
        <table class="mini-table">
          <thead><tr><th>Round</th><th>Step</th><th>Timing Output</th><th>Remaining Bad Ranges</th><th>Routes</th></tr></thead>
          <tbody>${renderTimingRounds(progress)}</tbody>
        </table>
      </div>
      <div class="full">
        <table class="mini-table">
          <thead><tr><th>Elapsed</th><th>Phase</th><th>Status</th><th>Round</th><th>Algorithm Detail</th></tr></thead>
          <tbody>${renderAlgorithmEvents(progress)}</tbody>
        </table>
      </div>
      <div class="full">
        <table class="mini-table">
          <thead><tr><th>Elapsed</th><th>Phase</th><th>Status</th><th>Info</th></tr></thead>
          <tbody>${eventRows}</tbody>
        </table>
      </div>
    </div>
  `;
}

function renderRunRows(run, logOpen = false, scheduleOpen = false) {
  const statusClass = run.status === "running" ? "running" : run.status === "failed" ? "failed" : "";
  const openViewer = run.trace_exists
    ? `<a href="${run.viewer_url}" target="_blank" rel="noreferrer">visualizer</a>`
    : `<span class="subtle">not ready</span>`;
  const duration = run.duration === null ? "-" : `${run.duration}s`;
  const mode = run.schedule ? "scheduled" : "unscheduled";
  const safeRunId = escapeHtml(run.id);
  return `
    <tr data-run-id="${safeRunId}">
      <td><span class="run-id">${safeRunId}</span></td>
      <td>${mode}</td>
      <td><span class="pill ${statusClass}">${run.status}</span></td>
      <td>${fixed(run.metrics?.psim_time, 0)}</td>
      <td>${fixed(run.metrics?.total_congested_time, 1)}</td>
      <td>${fixed(run.metrics?.average_fct, 2)}</td>
      <td>${fixed(run.metrics?.average_flow_bw, 2)}</td>
      <td>${fixed(run.metrics?.machine_utilization, 3)}</td>
      <td>${duration}</td>
      <td>${openViewer}</td>
      <td>${run.schedule ? `<button class="link-button toggle-schedule" data-run-id="${safeRunId}">${scheduleOpen ? "hide" : "progress"}</button>` : `<span class="subtle">-</span>`}</td>
      <td><button class="link-button toggle-log" data-run-id="${safeRunId}">${logOpen ? "hide" : "log"}</button></td>
      <td><button class="danger delete-run" data-run-id="${safeRunId}" ${run.status === "running" || run.status === "queued" ? "disabled" : ""}>Delete</button></td>
    </tr>
    <tr class="schedule-row" data-schedule-run-id="${safeRunId}" ${scheduleOpen ? "" : "hidden"}>
      <td colspan="13">${renderScheduleDetail(run)}</td>
    </tr>
    <tr class="log-row" data-log-run-id="${safeRunId}" ${logOpen ? "" : "hidden"}>
      <td colspan="13"><pre>${escapeHtml(logText(run))}</pre></td>
    </tr>
  `;
}

function updateRunRow(run) {
  ensureRunsTable();
  const body = $("runsBody");
  let row = body.querySelector(`tr[data-run-id="${CSS.escape(run.id)}"]`);
  let scheduleRow = body.querySelector(`tr[data-schedule-run-id="${CSS.escape(run.id)}"]`);
  let logRow = body.querySelector(`tr[data-log-run-id="${CSS.escape(run.id)}"]`);
  const scheduleOpen = scheduleRow ? !scheduleRow.hidden : false;
  const wasOpen = logRow ? !logRow.hidden : false;

  const wrapper = document.createElement("tbody");
  wrapper.innerHTML = renderRunRows(run, wasOpen, scheduleOpen).trim();
  const nextRow = wrapper.firstElementChild;
  const nextScheduleRow = nextRow.nextElementSibling;
  const nextLogRow = nextScheduleRow.nextElementSibling;

  if (row) {
    row.replaceWith(nextRow);
    if (scheduleRow) scheduleRow.replaceWith(nextScheduleRow);
    else nextRow.after(nextScheduleRow);
    if (logRow) logRow.replaceWith(nextLogRow);
    else nextScheduleRow.after(nextLogRow);
  } else {
    body.appendChild(nextRow);
    body.appendChild(nextScheduleRow);
    body.appendChild(nextLogRow);
  }
}

async function loadRuns(force = false) {
  if (polling || (refreshPaused && !force)) return;
  polling = true;
  try {
    const response = await fetch("/api/runs", { cache: "no-store" });
    const data = await response.json();
    if (refreshPaused && !force) return;

    if (!data.runs.length) {
      $("runs").innerHTML = `<span class="subtle">No runs yet.</span>`;
      tableReady = false;
    } else {
      ensureRunsTable();
      const seen = new Set(data.runs.map((run) => run.id));
      for (const run of data.runs) updateRunRow(run);
      for (const row of [...$("runsBody").querySelectorAll("tr[data-run-id]")]) {
        if (!seen.has(row.dataset.runId)) {
          $("runsBody").querySelector(`tr[data-schedule-run-id="${CSS.escape(row.dataset.runId)}"]`)?.remove();
          $("runsBody").querySelector(`tr[data-log-run-id="${CSS.escape(row.dataset.runId)}"]`)?.remove();
          row.remove();
        }
      }
    }

    const active = activeRunId ? data.runs.find((run) => run.id === activeRunId) : null;
    if (active) $("status").textContent = `${active.status} ${active.id}`;
    else $("status").textContent = "idle";
  } finally {
    polling = false;
  }
}

function toggleRefresh() {
  refreshPaused = !refreshPaused;
  const button = $("refreshToggle");
  button.textContent = refreshPaused ? "Resume refresh" : "Pause refresh";
  button.classList.toggle("paused", refreshPaused);
  button.setAttribute("aria-pressed", String(refreshPaused));

  if (!refreshPaused) loadRuns(true);
}

async function deleteRun(runId) {
  if (!confirm(`Delete results for ${runId}?`)) return;
  const response = await fetch(`/api/runs/${runId}`, { method: "DELETE" });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.error || `Delete failed with HTTP ${response.status}`);
  }
  if (activeRunId === runId) activeRunId = null;
  loadRuns();
}

async function startRun() {
  $("runButton").disabled = true;
  const payload = {
    schedule: $("schedule").checked,
    subflows: Number($("subflows").value),
    farid_rounds: Number($("faridRounds").value),
    timing_scheme: $("timingScheme").value,
    routing_fit_strategy: $("routingFitStrategy").value
  };
  const response = await fetch("/api/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const run = await response.json();
  activeRunId = run.id;
  $("runButton").disabled = false;
  loadRuns();
}

$("runButton").addEventListener("click", startRun);
$("refreshToggle").addEventListener("click", toggleRefresh);
$("runs").addEventListener("pointerover", (event) => {
  const trigger = event.target.closest(".pattern-trigger");
  if (trigger) showPatternGraph(trigger);
});
$("runs").addEventListener("click", (event) => {
  const scheduleToggle = event.target.closest(".toggle-schedule");
  if (scheduleToggle) {
    const scheduleRow = $("runsBody").querySelector(`tr[data-schedule-run-id="${CSS.escape(scheduleToggle.dataset.runId)}"]`);
    if (!scheduleRow) return;
    scheduleRow.hidden = !scheduleRow.hidden;
    scheduleToggle.textContent = scheduleRow.hidden ? "progress" : "hide";
    return;
  }

  const toggle = event.target.closest(".toggle-log");
  if (toggle) {
    const logRow = $("runsBody").querySelector(`tr[data-log-run-id="${CSS.escape(toggle.dataset.runId)}"]`);
    if (!logRow) return;
    logRow.hidden = !logRow.hidden;
    toggle.textContent = logRow.hidden ? "log" : "hide";
    return;
  }

  const button = event.target.closest(".delete-run");
  if (!button) return;
  deleteRun(button.dataset.runId);
});
$("schedule").addEventListener("change", () => {
  $("scheduledOptions").style.opacity = $("schedule").checked ? "1" : "0.55";
});
$("schedule").dispatchEvent(new Event("change"));

loadRuns();
setInterval(loadRuns, 1500);
