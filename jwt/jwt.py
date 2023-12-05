from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

import jwt


class JWTToken:
    def __init__(
        self,
        jwt_algorithm: str,
        jwt_secret: str,
        jwt_lifetime_hour: int,
    ):
        self.algorithm = jwt_algorithm
        self.secret = jwt_secret
        self.lifetime_hour = jwt_lifetime_hour

    @classmethod
    def from_config(
        cls,
        jwt_algorithm: str,
        jwt_secret: str,
        jwt_lifetime_hour: int = 1,
    ) -> "JWTToken":
        """Initialize object jwt token based on specified config
        :param jwt_algorithm: string algorithm that used
        :param jwt_secret: string secret in jwt
        :param jwt_lifetime_hour: lifetime hour of token
        :return: current object instance
        """
        return cls(jwt_algorithm, jwt_secret, jwt_lifetime_hour)

    def encode_token(self, data: Dict[str, Any]) -> Tuple[str, str]:
        """Generate token based on specified payload
        :param data: dictionary data
        :return: string token
        """
        if data.get("exp", None) is None:
            data["exp"] = datetime.utcnow() + timedelta(hours=self.lifetime_hour)

        # generate token with lifetime expired
        expiry_token = jwt.encode(
            payload=data,
            key=self.secret,
            algorithm=self.algorithm,
        )

        # generate 24 hours token lifetime
        _ = data.pop("exp", None)
        lifetime_token = jwt.encode(
            payload=data,
            key=self.secret,
            algorithm=self.algorithm,
        )
        return lifetime_token, expiry_token

    def decode_token(self, token: str) -> Dict[str, Any]:
        """Decode from string token to dictionary data
        :param token: string token
        :return: dictionary data
        """
        return jwt.decode(
            jwt=token.encode("utf-8"),
            key=self.secret,
            algorithms=[self.algorithm],
        )

    def is_token_is_valid(self, token: str) -> Tuple[bool, str]:
        """Check if token is valid or not
        :param token: string token
        :return: tuple data is valid and error message if any
        """
        try:
            resp = jwt.decode(
                token.encode("utf-8"),
                self.secret,
                algorithms=[self.algorithm],
            )
            if len(resp) > 0:
                return True, "valid"

            return False, "invalid token"
        except jwt.ExpiredSignatureError:
            # Signature has expired
            return False, "token is expired"
        except jwt.exceptions.DecodeError:
            return False, "invalid token"
