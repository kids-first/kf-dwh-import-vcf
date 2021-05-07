c.JupyterHub.admin_access = True
c.JupyterHub.authenticator_class = 'egojwtauthenticator.egojwtauthenticator.EgoAuthenticator'
c.EgoAuthenticator.signing_certificate = 'https://ego.kidsfirstdrc.org/oauth/token/public_key'
c.EgoAuthenticator.ego_allowed_user = 'c2f93d0f-0d3d-43e8-98e8-ab7a6f7fedb0'
c.EgoAuthenticator.cookie_name = 'EGO_JWT'

#  This is the address on which the proxy will bind. Sets protocol, ip, base_url
#  Default: 'http://:8000'
c.JupyterHub.bind_url = 'http://:8000/jupyter'
c.JupyterHub.hub_port = 8881

c.Spawner.default_url = '/lab'

c.Authenticator.admin_users = {'notebook_admin'}