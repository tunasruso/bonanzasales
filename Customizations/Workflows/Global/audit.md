# 🔍 Visual & Functional Quality Gate (/audit)

Description: Performs an elite-level UI/UX and logic audit to ensure compliance with 2026 "Visual Excellence" and "Responsible App" standards.

## Step 1: Environmental Check
* Access the local development URL via the internal browser.
* Verify build stability and ensure the Next.js 16 compiler has finished initial hydration. Elite UI/UX & logic audit for 2026 Visual Excellence. Features an autonomous Auto-Heal loop refactoring CSS/logic to a 9/10 score. Ensures Bento-grid consistency, robust interaction states, and Responsible App compliance.

## Step 2: Visual Excellence Audit
Analyze the UI for the following mandatory 2026 standards:
* Information Architecture (IA): Is the page scannable in under 3 seconds?
* Modular Bento Grid: Is the layout structured as a clean, high-density grid? Check spacing tokens for pixel-perfect consistency.
* Glassmorphism: Are backdrop-blur and transparency effects applied uniformly to cards and sidebars?
* Kinetic Typography: Ensure fonts are legible, responsive, and react to user interaction.
* Brand Consistency: Does the color palette and styling align with LiderTeks corporate identity?
* Accessibility (A11y): Check contrast ratios (WCAG 2.2) and keyboard navigation.

## Step 3: Interaction & Trust Audit
Perform a "stress test" on the User Experience (UX):
* Instant Feedback: Do all interactions acknowledge input in <100ms?
* System States:
    * Loading: Use of "Skeletons" during data fetching from SQL/Postgres.
    * Empty State: Clear Call-to-Action (CTA) when no data is available.
    * Error State: Non-blaming messages with a clear recovery path.
    * Success State: Tactile "Toasts" for completed actions.
* Optimistic UI: Do mutations update the UI immediately before server confirmation?
* Data Privacy: Ensure no raw SQL/1C error strings are exposed in the UI.

## Step 4: The Audit Report
Output a report with the following structure:
* 🚦 Squad Status:
    * Visual Score: [1-10]
    * Functional Score: [1-10]
    * Trust Score: [1-10]
* ✅ Visual Wins: Highlight outstanding UI elements.
* ❌ Critical Fails: List broken Bento grids, navigation noise, or A11y issues.
* 🪲 Logic & Trust Bugs: List failing endpoints, missing loading states, or ambiguous UX.

## Step 5: The Recursive Self-Correction Loop
Threshold: 9/10. If any category falls below 9:
1. Diagnose: Analyze "Critical Fails" and "Bugs" from Step 4.
2. Assign & Fix:
    * If Visual < 9: Assume "Design Lead" role and refactor CSS/Layout.
    * If Functional < 9: Assume "Builder" role and fix Logic or API endpoints.
3. Verify: Re-run the /audit command automatically.
4. Exit Condition: Stop only when all scores are ≥ 9 OR after 3 failed healing attempts (escalate to "Blocked" status).

## Step 6: Final Sync
Upon achieving a satisfactory result (Score ≥ 9):
* Update PLAN.md with status: "Verified & Polished".
* Commit code with prefix: [AUTO-HEALED].
