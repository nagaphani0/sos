import json
import re
import os
import tempfile
import hashlib
from urllib.parse import urlparse
from typing import Dict, Tuple, Optional

from CloudflareBypasser import CloudflareBypasser
from DrissionPage import ChromiumPage, ChromiumOptions
from pyvirtualdisplay import Display
from flask import Flask, request, jsonify
from flask_cors import CORS


class CloudflareBypassClient:
    """
    A unified client for bypassing Cloudflare protection and retrieving page content.
    
    Usage:
        client = CloudflareBypassClient()
        html, cookies, user_agent = client.get_page(url="https://example.com")
        # or
        cookies, user_agent = client.get_cookies(url="https://example.com")
    """
    
    def __init__(self, headless: bool = False, log: bool = True, docker_mode: bool = False):
        """
        Initialize the CloudflareBypassClient.
        
        Args:
            headless: Run browser in headless mode
            log: Enable logging
            docker_mode: Enable Docker-specific configurations
        """
        self.headless = headless or docker_mode
        self.log = log
        self.docker_mode = docker_mode or os.getenv("DOCKERMODE", "false").lower() == "true"
        self.browser_path = "/usr/bin/google-chrome" if self.docker_mode else None
        self.display = None
        
        # Chromium options arguments
        self.arguments = [
            "-no-first-run",
            "-force-color-profile=srgb",
            "-metrics-recording-only",
            "-password-store=basic",
            "-use-mock-keychain",
            "-export-tagged-pdf",
            "-no-default-browser-check",
            "-disable-background-mode",
            "-enable-features=NetworkService,NetworkServiceInProcess,LoadCryptoTokenExtension,PermuteTLSExtensions",
            "-disable-features=FlashDeprecationWarning,EnablePasswordsAccountStorage",
            "-deny-permission-prompts",
            "-disable-gpu",
            "-accept-lang=en-US",
        ]
        
        # Initialize display if headless mode is enabled
        if self.headless:
            self._init_display()
    
    def _init_display(self):
        """Initialize virtual display for headless mode."""
        try:
            self.display = Display(visible=0, size=(1920, 1080))
            self.display.start()
            self._log("Virtual display started for headless mode.")
        except Exception as e:
            self._log(f"Warning: Failed to start virtual display: {e}")
    
    def _log(self, message: str):
        """Log a message if logging is enabled."""
        if self.log:
            print(message)
    
    def _is_safe_url(self, url: str) -> bool:
        """Check if the URL is safe to access."""
        parsed_url = urlparse(url)
        ip_pattern = re.compile(
            r"^(127\.0\.0\.1|localhost|0\.0\.0\.0|::1|10\.\d+\.\d+\.\d+|172\.1[6-9]\.\d+\.\d+|172\.2[0-9]\.\d+\.\d+|172\.3[0-1]\.\d+\.\d+|192\.168\.\d+\.\d+)$"
        )
        hostname = parsed_url.hostname
        if (hostname and ip_pattern.match(hostname)) or parsed_url.scheme == "file":
            return False
        return True
    
    def _create_proxy_extension(self, username: str, password: str, endpoint: str, port: str) -> str:
        """Create a proxy extension for authentication."""
        temp_dir = tempfile.gettempdir()
        unique_proxy_id = hashlib.sha256(f"{username}:{password}:{endpoint}:{port}".encode()).hexdigest()
        directory_name = os.path.join(temp_dir, unique_proxy_id)
        
        if os.path.exists(directory_name):
            return directory_name
        
        manifest_json = """
        {
            "version": "1.0.0",
            "manifest_version": 2,
            "name": "Proxies",
            "permissions": [
                "proxy",
                "tabs",
                "unlimitedStorage",
                "storage",
                "<all_urls>",
                "webRequest",
                "webRequestBlocking"
            ],
            "background": {
                "scripts": ["background.js"]
            },
            "minimum_chrome_version":"22.0.0"
        }
        """

        background_js = """
        var config = {
                mode: "fixed_servers",
                rules: {
                  singleProxy: {
                    scheme: "http",
                    host: "%s",
                    port: parseInt(%s)
                  },
                  bypassList: ["localhost"]
                }
              };

        chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

        function callbackFn(details) {
            return {
                authCredentials: {
                    username: "%s",
                    password: "%s"
                }
            };
        }

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {urls: ["<all_urls>"]},
                    ['blocking']
        );
        """ % (endpoint, port, username, password)

        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        manifest_path = os.path.join(directory_name, "manifest.json")
        background_path = os.path.join(directory_name, "background.js")

        with open(manifest_path, "w") as file:
            file.write(manifest_json)
        
        with open(background_path, "w") as file2:
            file2.write(background_js)
        
        return directory_name
    
    def _setup_proxy(self, options: ChromiumOptions, proxy: Optional[str]):
        """Setup proxy configuration."""
        if not proxy:
            return
        
        try:
            parsed_proxy = urlparse(proxy)
            scheme = parsed_proxy.scheme.lower() if parsed_proxy.scheme else 'http'
            hostname = parsed_proxy.hostname
            port = parsed_proxy.port
            username = parsed_proxy.username
            password = parsed_proxy.password

            if not hostname or not port:
                raise ValueError("Proxy hostname or port missing")

            if scheme in ['http', 'https']:
                if username and password:
                    proxy_extension_path = self._create_proxy_extension(username, password, hostname, str(port))
                    options.add_extension(proxy_extension_path)
                elif not username and not password:
                    options.set_proxy(f"{scheme}://{hostname}:{port}")
                else:
                    raise ValueError("Proxy requires both username and password, or neither.")
            elif scheme.startswith('socks'):
                self._log(f"Warning: SOCKS proxy ({proxy}) is not supported due to chromium limitations.")
                raise NotImplementedError("SOCKS proxy is not supported")
            else:
                self._log(f"Warning: Unsupported proxy scheme '{scheme}'. Ignoring proxy.")

        except ValueError as e:
            self._log(f"Error parsing proxy string '{proxy}': {e}")
            raise
    
    def _create_driver(self, proxy: Optional[str] = None) -> ChromiumPage:
        """Create and configure a ChromiumPage driver."""
        options = ChromiumOptions().auto_port()
        options.set_paths(browser_path=self.browser_path).headless(self.headless)
        
        for arg in self.arguments:
            options.set_argument(arg)

        if self.docker_mode:
            options.set_argument("--auto-open-devtools-for-tabs", "true")
            options.set_argument("--no-sandbox")
            options.set_argument("--disable-gpu")
        
        self._setup_proxy(options, proxy)
        
        return ChromiumPage(addr_or_opts=options)
    
    def get_page(
        self, 
        url: str, 
        retries: int = 5, 
        proxy: Optional[str] = None
    ) -> Tuple[str, Dict[str, str], str]:
        """
        Get the HTML content of a page, bypassing Cloudflare if needed.
        
        Args:
            url: The URL to access
            retries: Number of retry attempts for Cloudflare bypass
            proxy: Optional proxy URL (format: scheme://[username:password@]host:port)
        
        Returns:
            Tuple of (html_content, cookies_dict, user_agent)
        
        Raises:
            ValueError: If URL is not safe
            Exception: If bypassing or content retrieval fails
        """
        if not self._is_safe_url(url):
            raise ValueError("Invalid or unsafe URL")
        
        driver = None
        try:
            self._log(f"Accessing URL: {url}")
            driver = self._create_driver(proxy)
            driver.get(url)
            
            # Bypass Cloudflare if needed
            cf_bypasser = CloudflareBypasser(driver, retries, self.log)
            cf_bypasser.bypass()
            
            # Extract content
            html = driver.html
            cookies = {cookie.get("name", ""): cookie.get("value", "") for cookie in driver.cookies()}
            user_agent = driver.user_agent
            
            self._log("Page content retrieved successfully.")
            return html, cookies, user_agent
            
        except Exception as e:
            self._log(f"Error accessing page: {e}")
            raise
        finally:
            if driver:
                driver.quit()
    
    def get_cookies(
        self, 
        url: str, 
        retries: int = 5, 
        proxy: Optional[str] = None
    ) -> Tuple[Dict[str, str], str]:
        """
        Get cookies and user agent from a page, bypassing Cloudflare if needed.
        
        Args:
            url: The URL to access
            retries: Number of retry attempts for Cloudflare bypass
            proxy: Optional proxy URL (format: scheme://[username:password@]host:port)
        
        Returns:
            Tuple of (cookies_dict, user_agent)
        
        Raises:
            ValueError: If URL is not safe
            Exception: If bypassing or content retrieval fails
        """
        if not self._is_safe_url(url):
            raise ValueError("Invalid or unsafe URL")
        
        driver = None
        try:
            self._log(f"Accessing URL: {url}")
            driver = self._create_driver(proxy)
            driver.get(url)
            
            # Bypass Cloudflare if needed
            cf_bypasser = CloudflareBypasser(driver, retries, self.log)
            cf_bypasser.bypass()
            
            # Extract cookies and user agent
            cookies = {cookie.get("name", ""): cookie.get("value", "") for cookie in driver.cookies()}
            user_agent = driver.user_agent
            
            self._log("Cookies retrieved successfully.")
            return cookies, user_agent
            
        except Exception as e:
            self._log(f"Error accessing page: {e}")
            raise
        finally:
            if driver:
                driver.quit()
    
    def cleanup(self):
        """Cleanup resources (virtual display, etc.)."""
        if self.display:
            try:
                self.display.stop()
                self._log("Virtual display stopped.")
            except Exception as e:
                self._log(f"Warning: Failed to stop virtual display: {e}")


# Simple example
# client = CloudflareBypassClient(headless=False, log=True)

# try:
#     # Get full page content
#     url = "https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/BirthDeath"  # Replace with your target URL
#     # url = "https://www.naukri.com/jobapi/v3/search?noOfResults=20&urlType=search_by_keyword&searchType=adv&keyword=python&pageNo=1&k=python&nignbevent_src=jobsearchDeskGNB&seoKey=python-jobs&src=jobsearchDesk&latLong="  # Replace with your target URL
#     html, cookies, user_agent = client.get_page(url)
    
#     print("=" * 80)
#     print(f"User Agent: {user_agent}")
#     print("=" * 80)
#     print(f"Cookies: {json.dumps(cookies, indent=2)}")
#     print("=" * 300)
#     print("HTML Content (first 500 chars):")
#     print(html[:1000])
#     print("=" * 80)
    
# except Exception as e:
#     print(f"Error: {e}")
# finally:
#     client.cleanup()


# Flask API Setup
app = Flask(__name__)
CORS(app)


@app.route('/api/get-page', methods=['POST'])
def get_page_api():
    """
    API endpoint to bypass Cloudflare and get page content.
    
    Expected JSON body:
    {
        "url": "https://example.com",
        "headless": true (optional, default: true),
        "docker_mode": false (optional, default: false)
    }
    
    Returns:
    {
        "success": true,
        "html": "page content",
        "cookies": {...},
        "user_agent": "...",
        "error": null
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'url' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing required field: url',
                'html': None,
                'cookies': None,
                'user_agent': None
            }), 400
        
        url = data.get('url')
        headless = data.get('headless', True)
        docker_mode = data.get('docker_mode', False)
        
        # Initialize client
        client = CloudflareBypassClient(headless=headless, log=False, docker_mode=docker_mode)
        
        try:
            # Get page content
            html, cookies, user_agent = client.get_page(url=url)
            
            return jsonify({
                'success': True,
                'html': html,
                'cookies': cookies,
                'user_agent': user_agent,
                'error': None
            }), 200
            
        finally:
            client.cleanup()
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'html': None,
            'cookies': None,
            'user_agent': None
        }), 500


@app.route('/api/get-cookies', methods=['POST'])
def get_cookies_api():
    """
    API endpoint to get Cloudflare cookies and user agent.
    
    Expected JSON body:
    {
        "url": "https://example.com",
        "headless": true (optional, default: true),
        "docker_mode": false (optional, default: false)
    }
    
    Returns:
    {
        "success": true,
        "cookies": {...},
        "user_agent": "...",
        "error": null
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'url' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing required field: url',
                'cookies': None,
                'user_agent': None
            }), 400
        
        url = data.get('url')
        headless = data.get('headless', True)
        docker_mode = data.get('docker_mode', False)
        
        # Initialize client
        client = CloudflareBypassClient(headless=headless, log=False, docker_mode=docker_mode)
        
        try:
            # Get cookies and user agent
            cookies, user_agent = client.get_cookies(url=url)
            
            return jsonify({
                'success': True,
                'cookies': cookies,
                'user_agent': user_agent,
                'error': None
            }), 200
            
        finally:
            client.cleanup()
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'cookies': None,
            'user_agent': None
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'ok', 'service': 'Cloudflare Bypass API'}), 200


if __name__ == '__main__':
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)
