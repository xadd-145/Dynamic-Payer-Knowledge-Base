// frontend/src/components/RuleCard.jsx
import { useState } from "react";
import client from "../api/client";
import VersionHistory from "./VersionHistory";

export default function RuleCard({ rule }) {
  const [expanded, setExpanded]   = useState(false);
  const [versions, setVersions]   = useState([]);
  const [loading, setLoading]     = useState(false);

  const toggleHistory = async () => {
    if (expanded) { setExpanded(false); return; }
    setLoading(true);
    try {
      const res = await client.get(`/rules/history/${rule.rule_code}`);
      setVersions(res.data);
      setExpanded(true);
    } catch {
      setExpanded(true);
    } finally {
      setLoading(false);
    }
  };

  const isSuperseded = !!rule.effective_end_date;

  return (
    <div className={`bg-white border rounded-xl p-5 shadow-sm ${
      isSuperseded ? "border-gray-200 opacity-75" : "border-teal-200"
    }`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className="bg-teal-600 text-white text-xs font-black px-2 py-1 rounded">
            {rule.rule_code}
          </span>
          <span className="bg-gray-100 text-gray-600 text-xs font-semibold px-2 py-1 rounded">
            {rule.version_label ?? "v1"}
          </span>
          {isSuperseded && (
            <span className="bg-orange-100 text-orange-700 text-xs font-semibold px-2 py-1 rounded">
              Superseded
            </span>
          )}
        </div>
        <button
          onClick={toggleHistory}
          className="text-xs text-teal-600 hover:underline font-semibold"
        >
          {loading ? "Loading..." : expanded ? "Hide History" : "Version History"}
        </button>
      </div>

      <p className="text-sm text-gray-800 leading-relaxed mb-3">{rule.rule_text ?? rule.normalized_rule_text}</p>

      <div className="text-xs text-gray-500 space-y-1">
        <p>
          <span className="font-semibold">Effective:</span>{" "}
          {rule.effective_start ?? rule.effective_start_date} →{" "}
          {rule.effective_end ?? rule.effective_end_date ?? "Present"}
        </p>
        {rule.rule_title && (
          <p><span className="font-semibold">Rule:</span> {rule.rule_title}</p>
        )}
      </div>

      {expanded && <VersionHistory versions={versions} />}
    </div>
  );
}