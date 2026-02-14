import json
import requests
from config import PERPLEXITY_API_KEY, TARGET_LOCATIONS


def scrape_company_roles(company_name, careers_url):
    """
    Use Perplexity API to search a company's careers page for relevant open roles.
    Returns a list of role dicts: {title, url, location, description, seniority, department}
    """
    locations_str = ", ".join(TARGET_LOCATIONS)

    prompt = f"""Search the careers/jobs page of {company_name} ({careers_url}) and find ALL currently open roles
that match ANY of these categories:
- Strategy (corporate strategy, business strategy, commercial strategy)
- Business Development
- Product Management / Product Strategy
- Management Consulting / Advisory
- AI Strategy / GenAI
- Commercial / GTM / Go-to-Market
- Operations Strategy
- Partnerships
- Chief of Staff / Founders Associate

Focus on roles in these locations: {locations_str}

For EACH role found, return a JSON array where each element has:
- "title": exact job title
- "url": direct link to the job posting (full URL)
- "location": city/country listed
- "description": 2-3 sentence summary of the role
- "seniority": inferred seniority level (Junior/Associate/Mid/Senior/Lead/Manager/Director/VP)
- "department": department or team name
- "posted_date": the date when the job was posted on the careers page (in format YYYY-MM-DD if available, or "Not specified" if not found)

Return ONLY the JSON array, no other text. If no matching roles are found, return an empty array [].
Be thorough; check multiple pages if the careers site has pagination.
Do NOT include engineering/software development roles unless they are specifically product management or strategy roles."""

    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "sonar-pro",
        "messages": [
            {
                "role": "system",
                "content": "You are a job search assistant. You search company careers pages and return structured JSON data about open positions. Always return valid JSON arrays."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 4000
    }

    response = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers=headers,
        json=payload,
        timeout=60
    )
    response.raise_for_status()

    content = response.json()["choices"][0]["message"]["content"]

    # Parse JSON from response - handle markdown code blocks
    content = content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        # Remove first line (```json) and last line (```)
        content = "\n".join(lines[1:-1])

    try:
        roles = json.loads(content)
        if not isinstance(roles, list):
            roles = []
    except json.JSONDecodeError:
        # Try to extract JSON array from the response
        start = content.find("[")
        end = content.rfind("]") + 1
        if start != -1 and end > start:
            try:
                roles = json.loads(content[start:end])
            except json.JSONDecodeError:
                roles = []
        else:
            roles = []

    return roles


def scrape_all_companies(companies):
    """Scrape roles for a list of company dicts. Returns {company_id: [roles]}."""
    results = {}
    for company in companies:
        try:
            roles = scrape_company_roles(company["name"], company["careers_url"])
            results[company["id"]] = {
                "company": company,
                "roles": roles,
                "error": None
            }
        except Exception as e:
            results[company["id"]] = {
                "company": company,
                "roles": [],
                "error": str(e)
            }
    return results
