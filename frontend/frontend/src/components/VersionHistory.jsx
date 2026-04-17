// frontend/src/components/VersionHistory.jsx
export default function VersionHistory({ versions }) {
  if (!versions || versions.length === 0) return null;
  return (
    <div className="mt-3 border-t pt-3">
      <p className="text-xs font-bold text-gray-500 uppercase mb-2">Version History</p>
      <div className="space-y-2">
        {versions.map((v) => (
          <div key={v.rule_version_id} className="text-xs bg-gray-50 rounded p-2 border">
            <div className="flex justify-between items-center mb-1">
              <span className="font-bold text-gray-700">v{v.version_number}</span>
              <span className={`px-2 py-0.5 rounded-full text-xs font-semibold ${
                v.effective_end_date ? "bg-gray-200 text-gray-600" : "bg-green-100 text-green-700"
              }`}>
                {v.effective_end_date ? "Superseded" : "Current"}
              </span>
            </div>
            <p className="text-gray-500">
              {v.effective_start_date} → {v.effective_end_date ?? "Present"}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}