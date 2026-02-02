from __future__ import annotations
import streamlit as st


def narrative(takeaway: str, evidence: str, so_what: str, next_steps: str) -> None:
    st.subheader("Narrative")
    st.markdown(f"**Takeaway:** {takeaway}")
    st.markdown(f"**Evidence:** {evidence}")
    st.markdown(f"**So what:** {so_what}")
    st.markdown(f"**Next steps:** {next_steps}")
