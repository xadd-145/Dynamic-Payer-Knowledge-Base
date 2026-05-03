// frontend/src/pages/StaffPortal.jsx
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import client from "../api/client";
import RuleCard from "../components/RuleCard";

export default function StaffPortal() {
  const navigate = useNavigate();
  const role = localStorage.getItem("role");

  const [topics, setTopics]       = useState([]);
  const [anchors, setAnchors]     = useState([]);
  const [topicId, setTopicId]     = useState("");
  const [date, setDate]           = useState("");
  const [dateType, setDateType]   = useState("date_of_service");
  const [anchorId, setAnchorId]   = useState("");
  const [results, setResults]     = useState(null);
  const [loading, setLoading]     = useState(false);
  const [error, setError]         = useState("");
  const [notifCount, setNotifCount] = useState(0);

  useEffect(() => {
    client.get("/rules/topics").then(r => setTopics(r.data));
    client.get("/rules/anchors").then(r => setAnchors(r.data));
  }, []);

  useEffect(() => {
    const poll = () => client.get("/notifications/unread")
      .then(r => setNotifCount(r.data.count)).catch(() => {});
    poll();
    const id = setInterval(poll, 30000);
    return () => clearInterval(id);
  }, []);

  const handleRetrieve = async () => {
    if (!topicId || !date) { setError("Select a billing topic and date."); return; }
    setLoading(true); setError(""); setResults(null);
    try {
      const body = { rule_topic_id: parseInt(topicId), query_date: date, query_date_type: dateType };
      if (anchorId) body.anchor_type_id = parseInt(anchorId);
      const res = await client.post("/rules/resolve", body);
      setResults(res.data);
    } catch {
      setError("Failed to retrieve rules. Check your connection.");
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    localStorage.clear();
    navigate("/login");
  };

  const rules = results?.results ?? [];
  const status = results?.resolution_status;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-teal-700 text-white px-6 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-black">DPKB — Staff Portal</h1>
          <p className="text-xs text-teal-200">NY Medicaid · UB-04 Facility Billing</p>
        </div>
        <div className="flex items-center gap-4">
          {/* CHANGED: notification banner now has Refresh Now button */}
          {notifCount > 0 && (
            <div className="flex items-center gap-2 bg-yellow-400 text-yellow-900 text-xs font-black px-3 py-1.5 rounded-full">
              <span>{notifCount} new rule{notifCount > 1 ? "s" : ""} published — please refresh</span>
              <button onClick={async () => {
                try { await client.post("/notifications/read"); } catch {}
                window.location.reload();
              }}
              className="underline hover:no-underline whitespace-nowrap"
              >
                Refresh Now
              </button>
            </div>
          )}
          <span className="text-xs text-teal-200 uppercase font-semibold">{role}</span>
          <button onClick={handleLogout}
            className="text-xs bg-teal-800 hover:bg-teal-900 px-3 py-1.5 rounded font-semibold">
            Sign Out
          </button>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-6 py-6 grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Left panel */}
        <div className="md:col-span-1">
          <div className="bg-white rounded-xl shadow-sm border p-5 space-y-4">
            <h2 className="font-black text-gray-800 text-sm uppercase tracking-wide">
              Query Rules
            </h2>

            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-1">Billing Topic *</label>
              <select value={topicId} onChange={e => setTopicId(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500">
                <option value="">Select topic...</option>
                {topics.map(t => (
                  <option key={t.rule_topic_id} value={t.rule_topic_id}>{t.topic_name}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-1">Date *</label>
              <input type="date" value={date} onChange={e => setDate(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
            </div>

            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-2">Date Type</label>
              <div className="space-y-1">
                {[["date_of_service", "Date of Service"], ["date_of_discharge", "Date of Discharge"]].map(([val, label]) => (
                  <label key={val} className="flex items-center gap-2 text-sm cursor-pointer">
                    <input type="radio" value={val} checked={dateType === val}
                      onChange={() => setDateType(val)} className="accent-teal-600" />
                    {label}
                  </label>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-xs font-semibold text-gray-600 mb-1">UB-04 Anchor Type (optional)</label>
              <select value={anchorId} onChange={e => setAnchorId(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500">
                <option value="">Any anchor type</option>
                {anchors.map(a => (
                  <option key={a.ub04_anchor_type_id} value={a.ub04_anchor_type_id}>{a.anchor_type_name}</option>
                ))}
              </select>
            </div>

            {error && <p className="text-xs text-red-600 font-semibold">{error}</p>}

            <button onClick={handleRetrieve} disabled={loading}
              className="w-full bg-teal-600 hover:bg-teal-700 text-white font-black py-2.5 rounded-lg text-sm transition disabled:opacity-50">
              {loading ? "Retrieving..." : "Retrieve Rules"}
            </button>
          </div>
        </div>

        {/* Right panel */}
        <div className="md:col-span-2">
          {results === null && !loading && (
            <div className="flex items-center justify-center h-64 text-gray-400 text-sm">
              Select a topic and date, then click Retrieve Rules.
            </div>
          )}

          {status === "no_match" && (
            <div className="bg-white rounded-xl border p-8 text-center">
              <p className="text-gray-500 font-semibold">No billing rules found</p>
              <p className="text-xs text-gray-400 mt-1">No rules were active for this topic on the selected date.</p>
            </div>
          )}

          {rules.length > 0 && (
            <div>
              <div className="flex items-center justify-between mb-4">
                <h2 className="font-black text-gray-800">
                  Results
                  <span className="ml-2 bg-teal-100 text-teal-800 text-xs font-black px-2 py-1 rounded-full">
                    {rules.length} rule{rules.length > 1 ? "s" : ""}
                  </span>
                </h2>
                <p className="text-xs text-gray-400">
                  As of {results.query_date} · {results.query_date_type?.replace("_", " ")}
                </p>
              </div>
              <div className="space-y-4">
                {rules.map(r => <RuleCard key={r.rule_version_id} rule={r} />)}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}