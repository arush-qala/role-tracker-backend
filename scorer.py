import json
import requests
from config import GEMINI_API_KEY

# Load profile once at module level
import os
_profile_path = os.path.join(os.path.dirname(__file__), "profile.json")
with open(_profile_path, "r") as f:
    PROFILE = json.load(f)


def score_role(role_title, role_description, role_location, role_seniority, company_name):
    """
    Use Gemini API to score a role against the candidate profile.
    Returns: {total_score: int, breakdown: dict, recommendation: str, reasoning: str}
    """
    profile_summary = json.dumps(PROFILE, indent=2)

    prompt = f"""You are an expert career advisor evaluating job fit for a candidate.

CANDIDATE PROFILE:
{profile_summary}

JOB TO EVALUATE:
- Company: {company_name}
- Title: {role_title}
- Location: {role_location}
- Seniority: {role_seniority}
- Description: {role_description}

SCORING FRAMEWORK (100 points total):
1. Hard Requirements (25 pts): Does the candidate meet experience years, visa/work authorization, required certifications, education requirements?
2. Core Skills Match (20 pts): How well do the candidate's core skills align with the role requirements?
3. Experience Relevance (20 pts): How relevant is the candidate's past work to this role?
4. Seniority Alignment (10 pts): Is the role at the right level for 8+ years experience with MBA?
5. Industry/Domain (10 pts): Does the candidate have relevant industry experience?
6. Preferred/Bonus Skills (10 pts): Does the candidate have nice-to-have skills listed?
7. Career Narrative (5 pts): Does this role make sense as a next step in the candidate's career?

RED FLAGS (auto-deductions):
- Missing hard requirement (e.g., required certification): -20 pts
- Significant experience gap (e.g., requires 10 yrs, has 8): -15 pts
- No relevant industry experience AND role explicitly requires it: -10 pts
- Overqualified (senior for junior role): -10 pts

Return ONLY a JSON object with:
{{
  "total_score": <integer 0-100>,
  "breakdown": {{
    "hard_requirements": <0-25>,
    "core_skills": <0-20>,
    "experience_relevance": <0-20>,
    "seniority_alignment": <0-10>,
    "industry_domain": <0-10>,
    "preferred_skills": <0-10>,
    "career_narrative": <0-5>
  }},
  "red_flags": ["list of any red flags applied"],
  "recommendation": "<Excellent|Good|Moderate|Weak|Poor>",
  "reasoning": "<2-3 sentence explanation of the score>"
}}"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-preview:generateContent?key={GEMINI_API_KEY}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 8000
        }
    }

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    content = response.json()["candidates"][0]["content"]["parts"][0]["text"]

    # Parse JSON from response
    content = content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1])

    try:
        result = json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}") + 1
        if start != -1 and end > start:
            result = json.loads(content[start:end])
        else:
            result = {
                "total_score": 0,
                "breakdown": {},
                "red_flags": ["Failed to parse scoring response"],
                "recommendation": "Error",
                "reasoning": "Could not parse Gemini response"
            }

    return result


def score_roles_batch(roles, company_name):
    """Score a batch of roles. Returns list of (role, score_result) tuples."""
    scored = []
    for role in roles:
        try:
            result = score_role(
                role_title=role.get("title", ""),
                role_description=role.get("description", ""),
                role_location=role.get("location", ""),
                role_seniority=role.get("seniority", ""),
                company_name=company_name
            )
            scored.append((role, result))
        except Exception as e:
            scored.append((role, {
                "total_score": 0,
                "breakdown": {},
                "red_flags": [f"Scoring error: {str(e)}"],
                "recommendation": "Error",
                "reasoning": f"Error during scoring: {str(e)}"
            }))
    return scored
