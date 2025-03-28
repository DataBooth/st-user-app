from propelauth_py import init_base_auth, UnauthorizedException


class Auth:
    def __init__(self, auth_url, integration_api_key):
        self.auth = init_base_auth(auth_url, integration_api_key)
        self.auth_url = auth_url
        self.integration_api_key = integration_api_key
        self.access_token = None

    def login(self, email_or_username, password):
        try:
            login_response = self.auth.login(email_or_username, password)
            self.access_token = login_response.access_token
            user = self.get_user(login_response.user_id)
            return user
        except UnauthorizedException:
            return None

    def get_user(self, user_id=None):
        try:
            if self.access_token is None:
                return self.force_refresh_user(user_id)
            return self.auth.validate_access_token_and_get_user(
                f"Bearer {self.access_token}"
            )
        except UnauthorizedException:
            return self.force_refresh_user(user_id)

    def force_refresh_user(self, user_id):
        access_token_response = self.auth.create_access_token(user_id, 10)
        self.access_token = access_token_response.access_token
        return self.auth.validate_access_token_and_get_user(
            f"Bearer {self.access_token}"
        )

    def get_account_url(self):
        return self.auth_url + "/account"

    def logout(self, user_id):
        self.auth.logout_all_user_sessions(user_id)
        self.access_token = None
