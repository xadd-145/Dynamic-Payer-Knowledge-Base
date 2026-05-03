// frontend/src/pages/SMEPortal.jsx
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import client from "../api/client";

// ---------------------------------------------------------------------------
// Compact collapsible tier section for SME
// ---------------------------------------------------------------------------

function SMETierSection({ title, items, expandedId, setExpandedId, renderExpanded, color }) {
  if (items.length === 0) return null;
  return (
    <section className="mb-6 rounded-2xl border shadow-sm overflow-hidden">
      <div className={`px-5 py-3 font-black text-sm flex justify-between items-center ${color}`}>
        <span>{title}</span>
        <span className="font-normal text-xs opacity-75">{items.length} fragment{items.length !== 1 ? "s" : ""} escalated by Admin</span>
      </div>
      <div className="overflow-x-auto bg-white">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="bg-gray-50 text-gray-500 text-left border-b">
              <th className="px-3 py-2">Score</th>
              <th className="px-3 py-2">ID</th>
              <th className="px-3 py-2">Source PDF</th>
              <th className="px-3 py-2">Page</th>
              <th className="px-3 py-2">Escalated By</th>
              <th className="px-3 py-2">Escalation Note</th>
              <th className="px-3 py-2">Preview</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <>
                <tr
                  key={item.policy_fragment_id}
                  className="border-b hover:bg-purple-50 cursor-pointer"
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
                  <td className="px-3 py-2">{item.reviewed_by ?? "—"}</td>
                  <td className="px-3 py-2 max-w-xs truncate text-gray-500">{item.review_notes ?? "—"}</td>
                  <td className="px-3 py-2 text-gray-600 max-w-xs truncate">
                    {(item.fragment_text_raw || "").slice(0, 80)}…
                  </td>
                </tr>
                {expandedId === item.policy_fragment_id && (
                  <tr key={`exp-${item.policy_fragment_id}`}>
                    <td colSpan="7" className="bg-gray-50 p-5">
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
// Main SMEPortal
// ---------------------------------------------------------------------------

export default function SMEPortal() {
  const navigate = useNavigate();
  const [fragments, setFragments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState(null);
  const [notes, setNotes] = useState({});
  const [msg, setMsg] = useState("");

  const loadQueue = () => {
    setLoading(true);
    client.get("/fragments/tier3")
      .then((r) => setFragments(r.data))
      .catch(() => setFragments([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => { loadQueue(); }, []);

  const handleSMEApprove = async (f) => {
    try {
      await client.post("/fragments/sme-approve", {
        policy_fragment_id: f.policy_fragment_id,
        notes: notes[f.policy_fragment_id] || null,
      });
      setMsg(`✓ Fragment #${f.policy_fragment_id} — Approval recommended. Admin will make the final publish decision.`);
      setExpandedId(null);
      loadQueue();
    } catch {
      setMsg("Error submitting recommendation.");
    }
  };

  const handleSMEReject = async (f) => {
    try {
      await client.post("/fragments/sme-reject", {
        policy_fragment_id: f.policy_fragment_id,
        notes: notes[f.policy_fragment_id] || null,
      });
      setMsg(`Fragment #${f.policy_fragment_id} — Rejection recommended. Admin will confirm.`);
      setExpandedId(null);
      loadQueue();
    } catch {
      setMsg("Error submitting recommendation.");
    }
  };

  const renderExpandedCard = (f) => (
    <div>
      <div className="mb-3 flex gap-2 items-center">
        <span className="bg-purple-100 text-purple-800 text-xs font-black px-2 py-1 rounded border border-purple-200">
          {(f.confidence_tier || "").replace("_", " ")} · Score: {f.confidence_score}
        </span>
        <span className="text-xs text-gray-400">Escalated by: {f.reviewed_by} at {f.reviewed_at}</span>
      </div>

      <p className="text-sm text-gray-800 mb-3 leading-relaxed">{f.fragment_text_raw}</p>

      <div className="text-xs text-gray-500 mb-4 space-y-1">
        <p><span className="font-semibold">Source:</span> {f.document_title} · p.{f.page_number_start}</p>
        <p><span className="font-semibold">Detected anchor:</span> {f.detected_anchor_type_code} = {f.detected_anchor_value}</p>
        <p><span className="font-semibold">Extracted date:</span> {f.extracted_effective_start_date ?? "None — please assess"}</p>
        <p><span className="font-semibold">Admin escalation note:</span> {f.review_notes ?? "None provided"}</p>
        {f.source_url && (
          <p>
            <span className="font-semibold">Source URL:</span>{" "}
            <a href={f.source_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">
              Open Source Document
            </a>
          </p>
        )}
      </div>

      <div className="mb-4">
        <label className="block text-xs font-semibold text-gray-600 mb-1">
          SME Interpretation Notes <span className="text-red-500">*</span>
        </label>
        <textarea
          rows={3}
          placeholder="Explain your recommendation. E.g. — This fragment describes a reimbursement rate change effective Jan 1. Applicable to outpatient therapy billing."
          value={notes[f.policy_fragment_id] || ""}
          onChange={(e) => setNotes((prev) => ({ ...prev, [f.policy_fragment_id]: e.target.value }))}
          className="w-full border border-gray-300 rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-purple-500 resize-none"
        />
        <p className="text-xs text-gray-400 mt-1">
          Your recommendation goes to Admin for final publish/reject decision. You are not publishing directly.
        </p>
      </div>

      <div className="flex gap-2">
        <button
          onClick={() => handleSMEApprove(f)}
          className="bg-purple-600 hover:bg-purple-700 text-white text-xs font-bold px-4 py-2 rounded"
        >
          Recommend Approval
        </button>
        <button
          onClick={() => handleSMEReject(f)}
          className="bg-red-500 hover:bg-red-600 text-white text-xs font-bold px-4 py-2 rounded"
        >
          Recommend Rejection
        </button>
      </div>
    </div>
  );

  const tier2 = fragments.filter((x) => x.confidence_tier === "TIER_2");
  const tier3 = fragments.filter((x) => x.confidence_tier === "TIER_3");

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-purple-800 text-white px-6 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-black">DPKB — SME Portal</h1>
          <p className="text-xs text-purple-200">Admin-Escalated Fragment Review</p>
        </div>
        <button
          onClick={() => { localStorage.clear(); navigate("/login"); }}
          className="text-xs bg-purple-900 hover:bg-purple-800 px-3 py-1.5 rounded font-semibold"
        >
          Sign Out
        </button>
      </div>

      <div className="max-w-6xl mx-auto px-6 py-6">
        {/* Governance notice */}
        <div className="mb-5 bg-purple-50 border border-purple-200 rounded-xl px-5 py-4 text-sm text-purple-800">
          <p className="font-black mb-1">SME Role — Expert Interpretation Only</p>
          <p className="text-xs text-purple-700">
            You see only fragments Admin has explicitly escalated. Your recommendations go back to Admin for final publish or reject decision. You do not publish directly.
          </p>
        </div>

        {msg && (
          <div className="mb-4 bg-purple-50 border border-purple-200 text-purple-800 px-4 py-3 rounded-lg text-sm font-semibold">
            {msg}
          </div>
        )}

        {loading && <p className="text-gray-400 text-sm">Loading SME queue...</p>}

        {!loading && fragments.length === 0 && (
          <div className="bg-white rounded-xl border p-8 text-center">
            <p className="text-gray-500 font-semibold">No fragments escalated to SME.</p>
            <p className="text-xs text-gray-400 mt-1">
              Admin must escalate fragments before they appear here.
            </p>
          </div>
        )}

        <SMETierSection
          title="TIER 2 — Escalated by Admin"
          items={tier2}
          expandedId={expandedId}
          setExpandedId={setExpandedId}
          renderExpanded={renderExpandedCard}
          color="bg-yellow-100 text-yellow-900"
        />

        <SMETierSection
          title="TIER 3 — Escalated by Admin"
          items={tier3}
          expandedId={expandedId}
          setExpandedId={setExpandedId}
          renderExpanded={renderExpandedCard}
          color="bg-red-100 text-red-900"
        />
      </div>
    </div>
  );
}
