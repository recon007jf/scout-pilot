def generate_briefing_html(lead_data, strategy_note):
    """
    Generates a Rich HTML Email mimicking a LinkedIn Post.
    
    Args:
        lead_data (dict): Contains 'name', 'title', 'company', 'snippet', 'image_url'.
        strategy_note (str): The 'Why this lead is hot' explanation.
    
    Returns:
        str: Raw HTML string.
    """
    
    # Placeholder URLs for assets (User to provide actual URLs later)
    HEADER_URL = "https://placeholder.com/header.png"
    FOOTER_URL = "https://placeholder.com/footer.png"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            .container {{
                width: 600px;
                margin: 0 auto;
                background-color: #ffffff;
                font-family: Arial, sans-serif;
            }}
            .header-img, .footer-img {{
                width: 100%;
                display: block;
            }}
            .linkedin-box {{
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                padding: 15px;
                margin: 20px;
                background-color: #ffffff;
            }}
            .profile-header {{
                display: flex;
                align-items: center;
                margin-bottom: 10px;
            }}
            .avatar {{
                width: 48px;
                height: 48px;
                border-radius: 50%;
                background-color: #ccc;
                margin-right: 10px;
            }}
            .name-block h3 {{
                margin: 0;
                font-size: 16px;
                color: #000;
            }}
            .name-block p {{
                margin: 0;
                font-size: 12px;
                color: #666;
            }}
            .snippet-text {{
                font-size: 14px;
                color: #333;
                line-height: 1.4;
            }}
            .strategy-box {{
                background-color: #e3f2fd; /* Light Blue */
                border-left: 4px solid #2196f3;
                padding: 15px;
                margin: 20px;
                font-size: 14px;
                color: #0d47a1;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Header -->
            <img src="{HEADER_URL}" alt="Point C Scout" class="header-img">
            
            <!-- Strategy Note -->
            <div class="strategy-box">
                <strong>🎯 Strategy Note:</strong><br>
                {strategy_note}
            </div>

            <!-- Synthetic LinkedIn Post -->
            <div class="linkedin-box">
                <div class="profile-header">
                    <div class="avatar">
                        <!-- Avatar Img Placeholder -->
                        <img src="{lead_data.get('image_url', '')}" style="width:100%; height:100%; border-radius:50%;" onerror="this.style.display='none'">
                    </div>
                    <div class="name-block">
                        <h3>{lead_data.get('name', 'Unknown Lead')}</h3>
                        <p>{lead_data.get('title', 'Title')} @ {lead_data.get('company', 'Company')}</p>
                        <p>2h • 🌐</p>
                    </div>
                </div>
                <div class="snippet-text">
                    "{lead_data.get('snippet', 'No snippet available.')}"
                </div>
            </div>

            <!-- Footer -->
            <img src="{FOOTER_URL}" alt="Point C Scout" class="footer-img">
        </div>
    </body>
    </html>
    """
    return html_content
