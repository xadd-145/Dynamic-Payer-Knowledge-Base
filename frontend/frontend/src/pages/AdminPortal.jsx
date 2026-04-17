// frontend/src/pages/AdminPortal.jsx
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import client from "../api/client";
import StaffPortal from "./StaffPortal";

export default function AdminPortal() {
  const navigate = useNavigate();
  const [fragments, setFragments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("queue"); // "queue" or "staff"
  const [form, setForm] = useState({});
  const [msg, setMsg] = useState("");

  const loadQueue = () => {
    setLoading(true);
    client.get("/fragments/tier2")
      .then(r => setFragments(r.data))
      .catch(() => setFragments([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => { loadQueue(); }, []);

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
    } catch { setMsg("Error approving fragment."); }
  };

  const handleEscalate = async (f) => {
    try {
      await client.post("/fragments/escalate", {
        policy_fragment_id: f.policy_fragment_id,
        notes: form[f.policy_fragment_id]?.notes || null,
      });
      setMsg("Fragment escalated to SME.");
      loadQueue();
    } catch { setMsg("Error escalating."); }
  };

  const handleReject = async (f) => {
    try {
      await client.post("/fragments/reject", {
        policy_fragment_id: f.policy_fragment_id,
        notes: form[f.policy_fragment_id]?.notes || null,
      });
      setMsg("Fragment rejected.");
      loadQueue();
    } catch { setMsg("Error rejecting."); }
  };

  const setField = (id, field, value) => {
    setForm(prev => ({ ...prev, [id]: { ...prev[id], [field]: value } }));
  };

  const triggerCrawl = async () => {
    try {
      const r = await client.post("/crawler/run");
      setMsg(`Crawler run complete. Run ID: ${r.data.run_id}`);
    } catch { setMsg("Crawler error."); }
  };

  if (view === "staff") return <StaffPortal />;

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-teal-800 text-white px-6 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-black">DPKB — Admin Portal</h1>
          <p className="text-xs text-teal-200">TIER 2 Fragment Review Queue</p>
        </div>
        <div className="flex gap-3">
          <button onClick={() => setView("staff")}
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

      <div className="max-w-5xl mx-auto px-6 py-6">
        {msg && (
          <div className="mb-4 bg-blue-50 border border-blue-200 text-blue-800 px-4 py-3 rounded-lg text-sm font-semibold">
            {msg}
          </div>
        )}

        {loading && <p className="text-gray-400 text-sm">Loading queue...</p>}

        {!loading && fragments.length === 0 && (
          <div className="bg-white rounded-xl border p-8 text-center">
            <p className="text-gray-500 font-semibold">No fragments in TIER 2 queue</p>
            <p className="text-xs text-gray-400 mt-1">All caught up.</p>
          </div>
        )}

        <div className="space-y-6">
          {fragments.map(f => {
            const fd = form[f.policy_fragment_id] || {};
            return (
              <div key={f.policy_fragment_id} className="bg-white rounded-xl border shadow-sm p-5">
                <div className="flex justify-between items-center mb-3">
                  <span className="bg-yellow-100 text-yellow-800 text-xs font-black px-2 py-1 rounded">
                    TIER 2 · Score: {f.confidence_score}
                  </span>
                  <span className="text-xs text-gray-400">Fragment #{f.policy_fragment_id}</span>
                </div>

                <p className="text-sm text-gray-800 mb-3 leading-relaxed">{f.fragment_text_raw}</p>

                <div className="text-xs text-gray-500 mb-4 space-y-1">
                  <p><span className="font-semibold">Source:</span> {f.document_title} · p.{f.page_number_start}</p>
                  <p><span className="font-semibold">Detected anchor:</span> {f.detected_anchor_type_code} = {f.detected_anchor_value}</p>
                  <p><span className="font-semibold">Extracted date:</span> {f.extracted_effective_start_date ?? "None detected"}</p>
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
                        onChange={e => setField(f.policy_fragment_id, field, e.target.value)}
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
          })}
        </div>
      </div>
    </div>
  );
}