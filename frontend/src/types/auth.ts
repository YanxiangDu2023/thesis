export type AuthUser = {
  id: number;
  full_name: string;
  email: string;
  created_at: string;
};

export type AuthResponse = {
  token: string;
  user: AuthUser;
};

export type CurrentUserResponse = {
  user: AuthUser;
};

export type LoginPayload = {
  email: string;
  password: string;
};

export type RegisterPayload = {
  full_name: string;
  email: string;
  password: string;
};
