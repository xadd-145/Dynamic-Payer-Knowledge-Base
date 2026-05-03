// frontend/src/pages/AdminPortal.jsx
import { useState, useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom";
import client from "../api/client";
import StaffPortal from "./StaffPortal";

function formatTier(tier) {
  if (!tier) return "UNKNOWN";
  return tier.replace("_", " ");
}

function groupByTier(items) {
  return {
    TIER_1: items.filter((x) => x.confidence_tier === "TIER_1"),
    TIER_2: items.filter((x) => x.confidence_tier === "TIER_2"),
    TIER_3: items.filter((x) => x.confidence_tier === "TIER_3"),
  };
}

const TIER_COLORS = {
  TIER_1: "bg-green-100 text-green-800 border-green-300",
  TIER_2: "bg-yellow-100 text-yellow-800 border-yellow-300",
  TIER_3: "bg-red-100 text-red-800 border-red-300",
};

const ACTION_BADGE = {
  approved:     "bg-green-100 text-green-800",
  rejected:     "bg-red-100 text-red-800",
  escalated:    "bg-orange-100 text-orange-800",
  sme_approved: "bg-purple-100 text-purple-800",
  sme_rejected: "bg-pink-100 text-pink-800",
};

function getAutoFill(f) {
  const tier = f.confidence_tier;
  const text = (f.fragment_text_raw || "").toLowerCase();
  const title = (f.document_title || "").toLowerCase();
  const combined = text + " " + title;

  let topic_code = "";

  if (combined.includes("emergency") || combined.includes(" er ") || combined.includes("emergency room")) {
    topic_code = "ER";
  } else if (combined.includes("inpatient") || combined.includes("discharge")) {
    topic_code = "INPT";
  } else if (
    combined.includes("therapy") || combined.includes("physical therapy") ||
    combined.includes("occupational therapy") || combined.includes("speech therapy") ||
    combined.includes(" pt ") || combined.includes(" ot ") || combined.includes(" st ")
  ) {
    topic_code = "THER";
  } else if (
    /\blab\b/.test(combined) || combined.includes("laboratory") ||
    combined.includes("radiology") || combined.includes("x-ray") || combined.includes("imaging")
  ) {
    topic_code = "LAB";
  } else if (
    combined.includes("outpatient") || combined.includes("clinic") ||
    combined.includes("ambulatory") || combined.includes("apg") ||
    combined.includes("fqhc") || combined.includes("mental health") ||
    combined.includes("pharmacy") || combined.includes("drug") || combined.includes("medication")
  ) {
    topic_code = "OUTPT";
  } else if (
    combined.includes("maternity") || combined.includes("doula") ||
    combined.includes("prenatal") || combined.includes("postpartum") ||
    combined.includes("obstetric") || combined.includes("labor") || combined.includes("delivery")
  ) {
    topic_code = "MATERNITY";
  }

  if (tier === "TIER_1") {
    return {
      rule_code: topic_code ? `${topic_code}-FRAG-${f.policy_fragment_id}` : `FRAG-${f.policy_fragment_id}`,
      rule_name: "",
      effective_start_date: f.extracted_effective_start_date || "",
      effective_end_date: "",
      topic_code,
      notes: "",
    };
  }
  if (tier === "TIER_2") {
    return {
      rule_code: "",
      rule_name: "",
      effective_start_date: f.extracted_effective_start_date || "",
      effective_end_date: "",
      topic_code,
      notes: "",
    };
  }
  return {};
}

function isFullyFilled(fd) {
  return !!(fd?.rule_code && fd?.rule_name && fd?.topic_code && fd?.effective_start_date);
}

// TIER_1 Card Component
function Tier1Card({ item, fd, onFieldChange, onApprove, onEscalate, onReject, gptLoading }) {
  const filled = isFullyFilled(fd);

  return (
    <div className={`rounded-xl border-2 shadow-sm p-4 mb-4 transition-all ${filled ? "border-green-400 bg-green-50" : "border-yellow-300 bg-yellow-50"}`}>
      <div className="flex justify-between items-center mb-2">
        <div className="flex gap-2 items-center">
          <span className="text-xs font-black px-2 py-1 rounded bg-green-100 text-green-800 border border-green-300">
            TIER 1 · Score: {item.confidence_score}
          </span>
          {gptLoading ? (
            <span className="text-xs text-blue-600 animate-pulse">⚡ GPT filling...</span>
          ) : filled ? (
            <span className="text-xs text-green-700 font-semibold">✓ Ready for One-Click Approve</span>
          ) : (
            <span className="text-xs text-yellow-700 font-semibold">⚠ Fill missing fields before approving</span>
          )}
        </div>
        <span className="text-xs text-gray-400">#{item.policy_fragment_id}</span>
      </div>

      <p className="text-xs text-gray-600 mb-1">
        <span className="font-semibold">Source:</span> {item.document_title} · p.{item.page_number_start}
        {item.source_url && (
          <> · <a href={item.source_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 underline ml-1">Open Source</a></>
        )}
      </p>
      <p className="text-xs text-gray-700 mb-3 line-clamp-2 leading-relaxed">{item.fragment_text_raw}</p>

      <div className="grid grid-cols-2 gap-2 mb-3">
        {[
          ["Rule Code *", "rule_code", "e.g. ER-001"],
          ["Rule Name *", "rule_name", "e.g. ER Multiple Visit Requirement"],
          ["Topic Code *", "topic_code", "e.g. ER"],
          ["Effective Start *", "effective_start_date", "YYYY-MM-DD"],
          ["Effective End", "effective_end_date", "YYYY-MM-DD or blank"],
          ["Notes", "notes", "Optional"],
        ].map(([label, field, placeholder]) => (
          <div key={field}>
            <label className="block text-xs font-semibold text-gray-600 mb-0.5">{label}</label>
            <input
              type="text"
              placeholder={placeholder}
              value={fd?.[field] || ""}
              onChange={(e) => onFieldChange(item.policy_fragment_id, field, e.target.value)}
              className={`w-full border rounded px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-teal-500 ${
                ["rule_code","rule_name","topic_code","effective_start_date"].includes(field) && !fd?.[field]
                  ? "border-yellow-400 bg-white"
                  : "border-gray-300 bg-white"
              }`}
            />
          </div>
        ))}
      </div>

      <div className="flex gap-2 flex-wrap">
  {filled ? (
    <button onClick={() => onApprove(item)}
      className="bg-green-600 hover:bg-green-700 text-white text-xs font-black px-4 py-2 rounded border-2 border-green-800">
      ⚡ One-Click Approve & Publish
    </button>
  ) : (
    <button onClick={() => onApprove(item)}
      className="bg-teal-600 hover:bg-teal-700 text-white text-xs font-bold px-4 py-2 rounded">
      Approve & Publish
    </button>
  )}
  <button onClick={() => onEscalate(item)}
    className="bg-orange-500 hover:bg-orange-600 text-white text-xs font-bold px-4 py-2 rounded">
    Escalate to SME
  </button>
  <button onClick={() => onReject(item)}
    className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded">
    Reject
  </button>
</div>
    </div>
  );
}

// TIER_2 / TIER_3 table section (unchanged)
function TierSection({ title, items, expandedId, onExpand, renderExpanded, color }) {
  if (items.length === 0) return null;
  return (
    <section className="mb-6 rounded-2xl border shadow-sm overflow-hidden">
      <div className={`px-5 py-3 font-black text-sm flex justify-between items-center ${color}`}>
        <span>{title}</span>
        <span className="font-normal text-xs opacity-75">{items.length} fragment{items.length !== 1 ? "s" : ""}</span>
      </div>
      <div className="overflow-x-auto bg-white">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="bg-gray-50 text-gray-500 text-left border-b">
              <th className="px-3 py-2">Score</th>
              <th className="px-3 py-2">ID</th>
              <th className="px-3 py-2">Source PDF</th>
              <th className="px-3 py-2">Page</th>
              <th className="px-3 py-2">Eff. Date</th>
              <th className="px-3 py-2">Preview</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <>
                <tr
                  key={item.policy_fragment_id}
                  className="border-b hover:bg-blue-50 cursor-pointer"
                  onClick={() => onExpand(item)}
                >
                  <td className="px-3 py-2 font-bold">{item.confidence_score}</td>
                  <td className="px-3 py-2 text-gray-400">#{item.policy_fragment_id}</td>
                  <td className="px-3 py-2">{item.document_title}</td>
                  <td className="px-3 py-2">{item.page_number_start ?? "—"}</td>
                  <td className="px-3 py-2">{item.extracted_effective_start_date ?? "—"}</td>
                  <td className="px-3 py-2 text-gray-600 max-w-xs truncate">
                    {(item.fragment_text_raw || "").slice(0, 100)}…
                  </td>
                </tr>
                {expandedId === item.policy_fragment_id && (
                  <tr key={`exp-${item.policy_fragment_id}`}>
                    <td colSpan="6" className="bg-gray-50 p-5">
                      {renderExpanded(item)}
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SMEResponseCard({ item, onFinalApprove, onFinalReject, form, setField }) {
  const fd = form[item.policy_fragment_id] || {};
  const isSMEApproved = item.review_status === "sme_approved";

  return (
    <div className={`bg-white rounded-xl border-2 shadow-sm p-5 mb-4 ${isSMEApproved ? "border-green-300" : "border-red-300"}`}>
      <div className="flex justify-between items-center mb-2">
        <span className={`text-xs font-black px-2 py-1 rounded ${isSMEApproved ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>
          SME {isSMEApproved ? "RECOMMENDED APPROVAL" : "RECOMMENDED REJECTION"} · {formatTier(item.confidence_tier)} · Score: {item.confidence_score}
        </span>
        <span className="text-xs text-gray-400">Fragment #{item.policy_fragment_id}</span>
      </div>
      <p className="text-sm text-gray-800 mb-2 leading-relaxed line-clamp-3">{item.fragment_text_raw}</p>
      <div className="text-xs text-gray-500 mb-3 space-y-1">
        <p><span className="font-semibold">Source:</span> {item.document_title} · p.{item.page_number_start}</p>
        <p><span className="font-semibold">SME Notes:</span> {item.review_notes || "None"}</p>
        <p><span className="font-semibold">Reviewed by:</span> {item.reviewed_by} at {item.reviewed_at}</p>
      </div>

      {isSMEApproved && (
        <div className="grid grid-cols-2 gap-2 mb-3">
          {[
            ["Rule Code", "rule_code", "e.g. THER-002"],
            ["Rule Name", "rule_name", "e.g. PT Modifier Requirement"],
            ["Topic Code", "topic_code", "e.g. THER"],
            ["Effective Start", "effective_start_date", "YYYY-MM-DD"],
            ["Effective End", "effective_end_date", "YYYY-MM-DD or blank"],
            ["Notes", "notes", "Optional"],
          ].map(([label, field, placeholder]) => (
            <div key={field}>
              <label className="block text-xs font-semibold text-gray-600 mb-1">{label}</label>
              <input
                type="text"
                placeholder={placeholder}
                value={fd[field] || ""}
                onChange={(e) => setField(item.policy_fragment_id, field, e.target.value)}
                className="w-full border border-gray-300 rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-teal-500"
              />
            </div>
          ))}
        </div>
      )}

      <div className="flex gap-2">
        {isSMEApproved && (
          <button onClick={() => onFinalApprove(item)}
            className="bg-teal-600 hover:bg-teal-700 text-white text-xs font-bold px-4 py-2 rounded">
            Approve & Publish
          </button>
        )}
        <button onClick={() => onFinalReject(item)}
          className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded">
          {isSMEApproved ? "Reject Anyway" : "Confirm Rejection"}
        </button>
      </div>
    </div>
  );
}

export default function AdminPortal() {
  const navigate = useNavigate();
  const [fragments, setFragments]       = useState([]);
  const [smeResponses, setSmeResponses] = useState([]);
  const [activityLog, setActivityLog]   = useState([]);
  const [loading, setLoading]           = useState(true);
  const [activeTab, setActiveTab]       = useState("pending");
  const [expandedId, setExpandedId]     = useState(null);
  const [form, setForm]                 = useState({});
  const [gptLoadingIds, setGptLoadingIds] = useState(new Set());
  const [msg, setMsg]                   = useState("");
  const gptFiredRef = useRef(new Set());

  const loadQueue = () => {
    setLoading(true);
    client.get("/fragments/tier2")
      .then((r) => setFragments(r.data))
      .catch(() => setFragments([]))
      .finally(() => setLoading(false));
  };

  const loadSMEResponses = () => {
    client.get("/fragments/sme-responses")
      .then((r) => setSmeResponses(r.data))
      .catch(() => setSmeResponses([]));
  };

  const loadActivityLog = () => {
    client.get("/fragments/activity-log")
      .then((r) => setActivityLog(r.data))
      .catch(() => setActivityLog([]));
  };

  useEffect(() => { loadQueue(); }, []);

  useEffect(() => {
    if (activeTab === "sme") loadSMEResponses();
    if (activeTab === "activity_log") loadActivityLog();
  }, [activeTab]);

  // Fire GPT for all TIER_1 sequentially after fragments load
  useEffect(() => {
    const tier1 = fragments.filter((f) => f.confidence_tier === "TIER_1");
    if (tier1.length === 0) return;

    // Apply baseline to all TIER_1 immediately
    setForm((prev) => {
      const updated = { ...prev };
      tier1.forEach((f) => {
        if (!updated[f.policy_fragment_id]) {
          updated[f.policy_fragment_id] = getAutoFill(f);
        }
      });
      return updated;
    });

    // Fire GPT sequentially
    const fireGPT = async () => {
      for (const f of tier1) {
        const id = f.policy_fragment_id;
        if (gptFiredRef.current.has(id)) continue;
        gptFiredRef.current.add(id);

        setGptLoadingIds((prev) => new Set([...prev, id]));
        try {
          const res = await client.post("/fragments/suggest", { policy_fragment_id: id });
          const s = res.data.suggestion;
          const baseline = getAutoFill(f);
          setForm((prev) => ({
            ...prev,
            [id]: {
              rule_code:            s.rule_code            || baseline.rule_code            || "",
              rule_name:            s.rule_name            || "",
              topic_code:           s.topic_code           || baseline.topic_code           || "",
              effective_start_date: s.effective_start_date || baseline.effective_start_date || "",
              effective_end_date:   s.effective_end_date   || "",
              notes:                s.notes                || "",
            },
          }));
        } catch {
          // GPT failed — baseline already set
        } finally {
          setGptLoadingIds((prev) => {
            const next = new Set(prev);
            next.delete(id);
            return next;
          });
        }
      }
    };

    fireGPT();
  }, [fragments]);

  const setField = (id, field, value) => {
    setForm((prev) => ({ ...prev, [id]: { ...prev[id], [field]: value } }));
  };

  const handleExpand = (item) => {
    const id = item.policy_fragment_id;
    if (expandedId === id) { setExpandedId(null); return; }
    setExpandedId(id);
    if (!form[id]) {
      const fill = getAutoFill(item);
      if (Object.keys(fill).length > 0) {
        setForm((prev) => ({ ...prev, [id]: fill }));
      }
    }
  };

  const handleApprove = async (f) => {
    const fd = form[f.policy_fragment_id] || {};
    if (!fd.rule_code || !fd.topic_code || !fd.effective_start_date) {
      setMsg("Fill in Rule Code, Topic Code, and Effective Start Date first.");
      return;
    }
    try {
      await client.post("/fragments/approve", {
        policy_fragment_id:   f.policy_fragment_id,
        rule_code:            fd.rule_code,
        rule_name:            fd.rule_name || fd.rule_code,
        topic_code:           fd.topic_code,
        effective_start_date: fd.effective_start_date,
        effective_end_date:   fd.effective_end_date || null,
        notes:                fd.notes || null,
      });
      setMsg(`✓ Rule ${fd.rule_code} published successfully.`);
      loadQueue();
      if (activeTab === "sme") loadSMEResponses();
    } catch (err) {
      const detail = err?.response?.data?.detail || "Error approving fragment.";
      setMsg(`Error: ${detail}`);
    }
  };

  const handleEscalate = async (f) => {
    try {
      await client.post("/fragments/escalate", {
        policy_fragment_id: f.policy_fragment_id,
        notes: form[f.policy_fragment_id]?.notes || null,
      });
      setMsg("Fragment escalated to SME.");
      setExpandedId(null);
      loadQueue();
    } catch (err) {
      const detail = err?.response?.data?.detail || "Error escalating.";
      setMsg(`Error: ${detail}`);
    }
  };

  const handleReject = async (f) => {
    try {
      await client.post("/fragments/reject", {
        policy_fragment_id: f.policy_fragment_id,
        notes: form[f.policy_fragment_id]?.notes || null,
      });
      setMsg("Fragment rejected.");
      setExpandedId(null);
      loadQueue();
      if (activeTab === "sme") loadSMEResponses();
    } catch (err) {
      const detail = err?.response?.data?.detail || "Error rejecting.";
      setMsg(`Error: ${detail}`);
    }
  };

  const triggerCrawl = async () => {
    try {
      const r = await client.post("/crawler/run");
      setMsg(`Crawler run complete. Run ID: ${r.data.run_id}`);
    } catch {
      setMsg("Crawler error.");
    }
  };

  const renderExpandedCard = (f) => {
    const fd = form[f.policy_fragment_id] || {};
    const isT2 = f.confidence_tier === "TIER_2";

    return (
      <div>
        <div className="mb-3 flex items-center gap-2">
          <span className={`text-xs font-black px-2 py-1 rounded border ${TIER_COLORS[f.confidence_tier] || "bg-gray-100 text-gray-800"}`}>
            {formatTier(f.confidence_tier)} · Score: {f.confidence_score}
          </span>
          {isT2 && (
            <span className="text-xs bg-blue-50 text-blue-700 border border-blue-200 px-2 py-1 rounded font-semibold">
              Auto-filled: Date + Topic
            </span>
          )}
        </div>

        <p className="text-sm text-gray-800 mb-3 leading-relaxed">{f.fragment_text_raw}</p>

        <div className="text-xs text-gray-500 mb-4 space-y-1">
          <p><span className="font-semibold">Source:</span> {f.document_title} · p.{f.page_number_start}</p>
          <p><span className="font-semibold">Extracted date:</span> {f.extracted_effective_start_date ?? "None detected"}</p>
          {f.source_url && (
            <p>
              <span className="font-semibold">Source URL:</span>{" "}
              <a href={f.source_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">
                Open Source Document
              </a>
            </p>
          )}
        </div>

        <div className="grid grid-cols-2 gap-3 mb-3">
          {[
            ["Rule Code", "rule_code", "e.g. THER-002"],
            ["Rule Name", "rule_name", "e.g. PT Modifier Requirement"],
            ["Topic Code", "topic_code", "e.g. THER"],
            ["Effective Start", "effective_start_date", "YYYY-MM-DD"],
            ["Effective End", "effective_end_date", "YYYY-MM-DD or leave blank"],
            ["Notes", "notes", "Optional review notes"],
          ].map(([label, field, placeholder]) => (
            <div key={field}>
              <label className="block text-xs font-semibold text-gray-600 mb-1">{label}</label>
              <input
                type="text"
                placeholder={placeholder}
                value={fd[field] || ""}
                onChange={(e) => setField(f.policy_fragment_id, field, e.target.value)}
                className="w-full border border-gray-300 rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-teal-500"
              />
            </div>
          ))}
        </div>

        <div className="flex gap-2">
          <button onClick={() => handleApprove(f)}
            className="bg-teal-600 hover:bg-teal-700 text-white text-xs font-bold px-4 py-2 rounded">
            Approve & Publish
          </button>
          <button onClick={() => handleEscalate(f)}
            className="bg-orange-500 hover:bg-orange-600 text-white text-xs font-bold px-4 py-2 rounded">
            Escalate to SME
          </button>
          <button onClick={() => handleReject(f)}
            className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded">
            Reject
          </button>
        </div>
      </div>
    );
  };

  if (activeTab === "staff") return <StaffPortal />;

  const grouped = groupByTier(fragments);
  const smeApproved = smeResponses.filter((x) => x.review_status === "sme_approved");
  const smeRejected = smeResponses.filter((x) => x.review_status === "sme_rejected");

  const tier1Ready = grouped.TIER_1.filter((f) => isFullyFilled(form[f.policy_fragment_id])).length;
  const tier1Partial = grouped.TIER_1.length - tier1Ready;

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-teal-800 text-white px-6 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-black">DPKB — Admin Portal</h1>
          <p className="text-xs text-teal-200">Fragment Review &amp; Policy Publishing</p>
        </div>
        <div className="flex gap-3">
          <button onClick={() => setActiveTab("staff")}
            className="text-xs bg-teal-700 hover:bg-teal-600 px-3 py-1.5 rounded font-semibold">
            Staff View
          </button>
          <button onClick={triggerCrawl}
            className="text-xs bg-green-700 hover:bg-green-600 px-3 py-1.5 rounded font-semibold">
            Trigger Crawler
          </button>
          <button onClick={() => { localStorage.clear(); navigate("/login"); }}
            className="text-xs bg-teal-900 hover:bg-teal-800 px-3 py-1.5 rounded font-semibold">
            Sign Out
          </button>
        </div>
      </div>

      <div className="border-b bg-white px-6">
        <div className="flex gap-6 max-w-6xl mx-auto">
          <button onClick={() => setActiveTab("pending")}
            className={`text-sm font-bold py-3 border-b-2 transition-colors ${
              activeTab === "pending" ? "border-teal-600 text-teal-700" : "border-transparent text-gray-400 hover:text-gray-600"
            }`}>
            Pending Review
            <span className="ml-2 bg-teal-100 text-teal-700 text-xs px-1.5 py-0.5 rounded font-semibold">
              {fragments.length}
            </span>
          </button>
          <button onClick={() => setActiveTab("sme")}
            className={`text-sm font-bold py-3 border-b-2 transition-colors ${
              activeTab === "sme" ? "border-purple-600 text-purple-700" : "border-transparent text-gray-400 hover:text-gray-600"
            }`}>
            SME Responses
            <span className="ml-2 bg-purple-100 text-purple-700 text-xs px-1.5 py-0.5 rounded font-semibold">
              {smeResponses.length}
            </span>
          </button>
          <button onClick={() => setActiveTab("activity_log")}
            className={`text-sm font-bold py-3 border-b-2 transition-colors ${
              activeTab === "activity_log" ? "border-gray-700 text-gray-800" : "border-transparent text-gray-400 hover:text-gray-600"
            }`}>
            Activity Log
            <span className="ml-2 bg-gray-100 text-gray-600 text-xs px-1.5 py-0.5 rounded font-semibold">
              {activityLog.length}
            </span>
          </button>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-6 py-6">
        {msg && (
          <div className="mb-4 bg-blue-50 border border-blue-200 text-blue-800 px-4 py-3 rounded-lg text-sm font-semibold">
            {msg}
          </div>
        )}

        {activeTab === "pending" && (
          <>
            {loading && <p className="text-gray-400 text-sm">Loading queue...</p>}
            {!loading && fragments.length === 0 && (
              <div className="bg-white rounded-xl border p-8 text-center">
                <p className="text-gray-500 font-semibold">No pending fragments.</p>
                <p className="text-xs text-gray-400 mt-1">All caught up.</p>
              </div>
            )}

            {/* TIER_1 — Card Grid */}
            {grouped.TIER_1.length > 0 && (
              <section className="mb-8">
                <div className="flex items-center gap-3 mb-4">
                  <h2 className="text-base font-black text-green-900">
                    TIER 1 — Strongest AI Confidence ({grouped.TIER_1.length})
                  </h2>
                  <span className="text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded font-semibold">
                    ✓ {tier1Ready} ready
                  </span>
                  {tier1Partial > 0 && (
                    <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded font-semibold">
                      ⚠ {tier1Partial} need fields
                    </span>
                  )}
                  {gptLoadingIds.size > 0 && (
                    <span className="text-xs text-blue-600 animate-pulse">
                      ⚡ GPT filling {gptLoadingIds.size} remaining...
                    </span>
                  )}
                </div>
                {grouped.TIER_1.map((item) => (
                  <Tier1Card
                    key={item.policy_fragment_id}
                    item={item}
                    fd={form[item.policy_fragment_id] || {}}
                    onFieldChange={setField}
                    onApprove={handleApprove}
                    onEscalate={handleEscalate}
                    onReject={handleReject}
                    gptLoading={gptLoadingIds.has(item.policy_fragment_id)}
                  />
                ))}
              </section>
            )}

            {/* TIER_2 and TIER_3 — existing table */}
            <TierSection title="TIER 2 Candidates — Moderate Confidence"
              items={grouped.TIER_2} expandedId={expandedId} onExpand={handleExpand}
              renderExpanded={renderExpandedCard} color="bg-yellow-100 text-yellow-900" />
            <TierSection title="TIER 3 Candidates — Low Confidence / Ambiguous"
              items={grouped.TIER_3} expandedId={expandedId} onExpand={handleExpand}
              renderExpanded={renderExpandedCard} color="bg-red-100 text-red-900" />
          </>
        )}

        {activeTab === "sme" && (
          <>
            {smeResponses.length === 0 && (
              <div className="bg-white rounded-xl border p-8 text-center">
                <p className="text-gray-500 font-semibold">No SME responses yet.</p>
                <p className="text-xs text-gray-400 mt-1">Escalate fragments from Pending Review to see them here after SME review.</p>
              </div>
            )}
            {smeApproved.length > 0 && (
              <section className="mb-8">
                <h2 className="text-base font-black text-green-800 mb-3">SME Recommended Approval ({smeApproved.length})</h2>
                {smeApproved.map((item) => (
                  <SMEResponseCard key={item.policy_fragment_id} item={item}
                    onFinalApprove={handleApprove} onFinalReject={handleReject}
                    form={form} setField={setField} />
                ))}
              </section>
            )}
            {smeRejected.length > 0 && (
              <section>
                <h2 className="text-base font-black text-red-800 mb-3">SME Recommended Rejection ({smeRejected.length})</h2>
                {smeRejected.map((item) => (
                  <SMEResponseCard key={item.policy_fragment_id} item={item}
                    onFinalApprove={handleApprove} onFinalReject={handleReject}
                    form={form} setField={setField} />
                ))}
              </section>
            )}
          </>
        )}

        {activeTab === "activity_log" && (
          <>
            {activityLog.length === 0 && (
              <div className="bg-white rounded-xl border p-8 text-center">
                <p className="text-gray-500 font-semibold">No activity yet.</p>
                <p className="text-xs text-gray-400 mt-1">Actions will appear here after fragments are approved, rejected, or escalated.</p>
              </div>
            )}
            {activityLog.length > 0 && (
              <div className="bg-white rounded-2xl border shadow-sm overflow-hidden">
                <div className="px-5 py-3 bg-gray-800 text-white font-black text-sm">
                  Full Activity Log — All Fragment Actions
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="bg-gray-50 text-gray-500 text-left border-b">
                        <th className="px-3 py-2">Fragment ID</th>
                        <th className="px-3 py-2">Action</th>
                        <th className="px-3 py-2">By</th>
                        <th className="px-3 py-2">Source PDF</th>
                        <th className="px-3 py-2">Notes</th>
                        <th className="px-3 py-2">Timestamp</th>
                      </tr>
                    </thead>
                    <tbody>
                      {activityLog.map((row) => (
                        <tr key={row.policy_fragment_id} className="border-b hover:bg-gray-50">
                          <td className="px-3 py-2 text-gray-400">#{row.policy_fragment_id}</td>
                          <td className="px-3 py-2">
                            <span className={`px-2 py-0.5 rounded text-xs font-black ${ACTION_BADGE[row.action] || "bg-gray-100 text-gray-700"}`}>
                              {row.action?.toUpperCase()}
                            </span>
                          </td>
                          <td className="px-3 py-2">{row.reviewed_by || "—"}</td>
                          <td className="px-3 py-2">{row.document_title}</td>
                          <td className="px-3 py-2 text-gray-500 max-w-xs truncate">{row.review_notes || "—"}</td>
                          <td className="px-3 py-2 text-gray-400">{row.reviewed_at || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}