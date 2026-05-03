# backend/utils/email_helper.py
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def _split_emails(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _send(to_emails: list[str], subject: str, body: str) -> bool:
    """Send email via Gmail SMTP. Fails silently — never blocks DPKB workflow."""
    try:
        gmail_user     = os.getenv("GMAIL_USER")
        gmail_password = os.getenv("GMAIL_APP_PASSWORD")

        if not gmail_user or not gmail_password or not to_emails:
            print("[EMAIL] Skipped — missing GMAIL_USER, GMAIL_APP_PASSWORD, or recipient list.")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = gmail_user
        msg["To"]      = ", ".join(to_emails)
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, to_emails, msg.as_string())

        print(f"[EMAIL] Sent: {subject}")
        return True

    except Exception as e:
        print(f"[EMAIL] Skipped/failed: {e}")
        return False


def notify_admin_new_fragments(count: int, source: str):
    admin_emails = _split_emails(os.getenv("DPKB_ADMIN_EMAILS"))
    _send(
        to_emails=admin_emails,
        subject=f"DPKB Admin: {count} New Fragment(s) Pending Review",
        body=(
            "<h3>New Fragments in Your Queue</h3>"
            f"<p><strong>{count} new policy fragment(s)</strong> are ready for your review.</p>"
            f"<p><strong>Source:</strong> {source}</p>"
            "<p>Log in to the <strong>Admin Portal → Pending Review</strong> to action them.</p>"
        ),
    )


def notify_sme_escalation(fragment_id: int, escalated_by: str, notes: str | None = None):
    sme_emails = _split_emails(os.getenv("DPKB_SME_EMAILS"))
    _send(
        to_emails=sme_emails,
        subject=f"DPKB SME: Fragment #{fragment_id} Needs Your Review",
        body=(
            "<h3>Fragment Escalated for SME Review</h3>"
            "<p>An Admin has escalated a policy fragment that requires your expert interpretation.</p>"
            f"<p><strong>Fragment ID:</strong> #{fragment_id}</p>"
            f"<p><strong>Escalated by:</strong> {escalated_by}</p>"
            f"<p><strong>Admin Notes:</strong> {notes or 'None'}</p>"
            "<p>Log in to the <strong>SME Portal</strong> to submit your recommendation.</p>"
        ),
    )


def notify_admin_sme_response(
    fragment_id: int,
    recommendation: str,
    sme_user: str,
    notes: str | None = None,
):
    admin_emails = _split_emails(os.getenv("DPKB_ADMIN_EMAILS"))
    _send(
        to_emails=admin_emails,
        subject=f"DPKB Admin: SME Response Ready — Fragment #{fragment_id}",
        body=(
            "<h3>SME Recommendation Submitted</h3>"
            "<p>An SME has reviewed a fragment and submitted their recommendation.</p>"
            f"<p><strong>Fragment ID:</strong> #{fragment_id}</p>"
            f"<p><strong>Recommendation:</strong> <strong>{recommendation.upper()}</strong></p>"
            f"<p><strong>Reviewed by:</strong> {sme_user}</p>"
            f"<p><strong>SME Notes:</strong> {notes or 'None'}</p>"
            "<p>Log in to the <strong>Admin Portal → SME Responses</strong> to make the final decision.</p>"
        ),
    )