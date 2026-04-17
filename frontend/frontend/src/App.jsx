// frontend/src/App.jsx
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Login       from "./pages/Login";
import StaffPortal from "./pages/StaffPortal";
import AdminPortal from "./pages/AdminPortal";
import SMEPortal   from "./pages/SMEPortal";

function Protected({ children, roles }) {
  const token = localStorage.getItem("token");
  const role  = localStorage.getItem("role");
  if (!token) return <Navigate to="/login" replace />;
  if (roles && !roles.includes(role)) return <Navigate to="/login" replace />;
  return children;
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/staff" element={
          <Protected roles={["staff","admin","sme"]}><StaffPortal /></Protected>
        } />
        <Route path="/admin" element={
          <Protected roles={["admin"]}><AdminPortal /></Protected>
        } />
        <Route path="/sme" element={
          <Protected roles={["sme"]}><SMEPortal /></Protected>
        } />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    </BrowserRouter>
  );
}