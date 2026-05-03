// frontend/src/pages/AdminPortal.jsx
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import client from "../api/client";
import StaffPortal from "./StaffPortal";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Compact collapsible tier section
// ---------------------------------------------------------------------------

function TierSection({ title, items, expandedId, setExpandedId, renderExpanded, color }) {
  if (items.length === 0) return null;
  return (
    <section className={`mb-6 rounded-2xl border shadow-sm overflow-hidden`}>
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
                  onClick={() =>
                    setExpandedId(
                      expandedId === item.policy_fragment_id ? null : item.policy_fragment_id
                    )
                  }
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

// ---------------------------------------------------------------------------
// SME Responses section
// ---------------------------------------------------------------------------

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
          <button
            onClick={() => onFinalApprove(item)}
            className="bg-teal-600 hover:bg-teal-700 text-white text-xs font-bold px-4 py-2 rounded"
          >
            Approve & Publish
          </button>
        )}
        <button
          onClick={() => onFinalReject(item)}
          className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded"
        >
          {isSMEApproved ? "Reject Anyway" : "Confirm Rejection"}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main AdminPortal
// ---------------------------------------------------------------------------

export default function AdminPortal() {
  const navigate = useNavigate();
  const [fragments, setFragments] = useState([]);
  const [smeResponses, setSmeResponses] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState("pending"); // "pending" | "sme" | "staff"
  const [expandedId, setExpandedId] = useState(null);
  const [form, setForm] = useState({});
  const [msg, setMsg] = useState("");

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

  useEffect(() => {
    loadQueue();
  }, []);

  useEffect(() => {
    if (activeTab === "sme") loadSMEResponses();
  }, [activeTab]);

  const setField = (id, field, value) => {
    setForm((prev) => ({ ...prev, [id]: { ...prev[id], [field]: value } }));
  };

  const handleApprove = async (f) => {
    const fd = form[f.policy_fragment_id] || {};
    if (!fd.rule_code || !fd.topic_code || !fd.effective_start_date) {
      setMsg("Fill in Rule Code, Topic Code, and Effective Start Date first.");
      return;
    }
    try {
      await client.post("/fragments/approve", {
        policy_fragment_id: f.policy_fragment_id,
        rule_code: fd.rule_code,
        rule_name: fd.rule_name || fd.rule_code,
        topic_code: fd.topic_code,
        effective_start_date: fd.effective_start_date,
        effective_end_date: fd.effective_end_date || null,
        notes: fd.notes || null,
      });
      setMsg(`✓ Rule ${fd.rule_code} published successfully.`);
      loadQueue();
      if (activeTab === "sme") loadSMEResponses();
    } catch {
      setMsg("Error approving fragment.");
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
    } catch {
      setMsg("Error escalating.");
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
    } catch {
      setMsg("Error rejecting.");
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

  // Expanded card for pending review
  const renderExpandedCard = (f) => {
    const fd = form[f.policy_fragment_id] || {};
    return (
      <div>
        <div className="mb-3">
          <span className={`text-xs font-black px-2 py-1 rounded border ${TIER_COLORS[f.confidence_tier] || "bg-gray-100 text-gray-800"}`}>
            {formatTier(f.confidence_tier)} · Score: {f.confidence_score}
          </span>
        </div>
        <p className="text-sm text-gray-800 mb-3 leading-relaxed">{f.fragment_text_raw}</p>
        <div className="text-xs text-gray-500 mb-4 space-y-1">
          <p><span className="font-semibold">Source:</span> {f.document_title} · p.{f.page_number_start}</p>
          <p><span className="font-semibold">Detected anchor:</span> {f.detected_anchor_type_code} = {f.detected_anchor_value}</p>
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
          <button
            onClick={() => handleApprove(f)}
            className="bg-teal-600 hover:bg-teal-700 text-white text-xs font-bold px-4 py-2 rounded"
          >
            Approve & Publish
          </button>
          <button
            onClick={() => handleEscalate(f)}
            className="bg-orange-500 hover:bg-orange-600 text-white text-xs font-bold px-4 py-2 rounded"
          >
            Escalate to SME
          </button>
          <button
            onClick={() => handleReject(f)}
            className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded"
          >
            Reject
          </button>
        </div>
      </div>
    );
  };

  if (activeTab === "staff") {
    return <StaffPortal />;
  }

  const grouped = groupByTier(fragments);
  const smeApproved = smeResponses.filter((x) => x.review_status === "sme_approved");
  const smeRejected = smeResponses.filter((x) => x.review_status === "sme_rejected");

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-teal-800 text-white px-6 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-black">DPKB — Admin Portal</h1>
          <p className="text-xs text-teal-200">Fragment Review &amp; Policy Publishing</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setActiveTab("staff")}
            className="text-xs bg-teal-700 hover:bg-teal-600 px-3 py-1.5 rounded font-semibold"
          >
            Staff View
          </button>
          <button
            onClick={triggerCrawl}
            className="text-xs bg-green-700 hover:bg-green-600 px-3 py-1.5 rounded font-semibold"
          >
            Trigger Crawler
          </button>
          <button
            onClick={() => { localStorage.clear(); navigate("/login"); }}
            className="text-xs bg-teal-900 hover:bg-teal-800 px-3 py-1.5 rounded font-semibold"
          >
            Sign Out
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b bg-white px-6">
        <div className="flex gap-6 max-w-6xl mx-auto">
          <button
            onClick={() => setActiveTab("pending")}
            className={`text-sm font-bold py-3 border-b-2 transition-colors ${
              activeTab === "pending" ? "border-teal-600 text-teal-700" : "border-transparent text-gray-400 hover:text-gray-600"
            }`}
          >
            Pending Review
            <span className="ml-2 bg-teal-100 text-teal-700 text-xs px-1.5 py-0.5 rounded font-semibold">
              {fragments.length}
            </span>
          </button>
          <button
            onClick={() => setActiveTab("sme")}
            className={`text-sm font-bold py-3 border-b-2 transition-colors ${
              activeTab === "sme" ? "border-purple-600 text-purple-700" : "border-transparent text-gray-400 hover:text-gray-600"
            }`}
          >
            SME Responses
            <span className="ml-2 bg-purple-100 text-purple-700 text-xs px-1.5 py-0.5 rounded font-semibold">
              {smeResponses.length}
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

        {/* PENDING REVIEW TAB */}
        {activeTab === "pending" && (
          <>
            {loading && <p className="text-gray-400 text-sm">Loading queue...</p>}
            {!loading && fragments.length === 0 && (
              <div className="bg-white rounded-xl border p-8 text-center">
                <p className="text-gray-500 font-semibold">No pending fragments.</p>
                <p className="text-xs text-gray-400 mt-1">All caught up.</p>
              </div>
            )}
            <TierSection
              title="TIER 1 Candidates — Strongest AI Confidence"
              items={grouped.TIER_1}
              expandedId={expandedId}
              setExpandedId={setExpandedId}
              renderExpanded={renderExpandedCard}
              color="bg-green-100 text-green-900"
            />
            <TierSection
              title="TIER 2 Candidates — Moderate Confidence"
              items={grouped.TIER_2}
              expandedId={expandedId}
              setExpandedId={setExpandedId}
              renderExpanded={renderExpandedCard}
              color="bg-yellow-100 text-yellow-900"
            />
            <TierSection
              title="TIER 3 Candidates — Low Confidence / Ambiguous"
              items={grouped.TIER_3}
              expandedId={expandedId}
              setExpandedId={setExpandedId}
              renderExpanded={renderExpandedCard}
              color="bg-red-100 text-red-900"
            />
          </>
        )}

        {/* SME RESPONSES TAB */}
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
                <h2 className="text-base font-black text-green-800 mb-3">
                  SME Recommended Approval ({smeApproved.length})
                </h2>
                {smeApproved.map((item) => (
                  <SMEResponseCard
                    key={item.policy_fragment_id}
                    item={item}
                    onFinalApprove={handleApprove}
                    onFinalReject={handleReject}
                    form={form}
                    setField={setField}
                  />
                ))}
              </section>
            )}

            {smeRejected.length > 0 && (
              <section>
                <h2 className="text-base font-black text-red-800 mb-3">
                  SME Recommended Rejection ({smeRejected.length})
                </h2>
                {smeRejected.map((item) => (
                  <SMEResponseCard
                    key={item.policy_fragment_id}
                    item={item}
                    onFinalApprove={handleApprove}
                    onFinalReject={handleReject}
                    form={form}
                    setField={setField}
                  />
                ))}
              </section>
            )}
          </>
        )}
      </div>
    </div>
  );
}
