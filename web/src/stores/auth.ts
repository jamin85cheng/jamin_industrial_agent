import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface User {
  user_id: string
  username: string
  email?: string
  role: string
  roles: string[]
  tenant_id?: string | null
  permissions?: string[]
}

interface AuthState {
  isAuthenticated: boolean
  token: string | null
  refreshToken: string | null
  tokenExpiresAt: number | null
  refreshTokenExpiresAt: number | null
  user: User | null
  login: (
    token: string,
    refreshToken: string | null,
    user: User,
    expiresInSeconds?: number,
    refreshExpiresInSeconds?: number,
  ) => void
  updateTokens: (
    token: string,
    refreshToken: string | null,
    expiresInSeconds?: number,
    refreshExpiresInSeconds?: number,
  ) => void
  logout: () => void
  updateUser: (user: Partial<User>) => void
}

const resolveExpiry = (seconds?: number) => (typeof seconds === 'number' ? Date.now() + seconds * 1000 : null)

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      isAuthenticated: false,
      token: null,
      refreshToken: null,
      tokenExpiresAt: null,
      refreshTokenExpiresAt: null,
      user: null,
      login: (token, refreshToken, user, expiresInSeconds, refreshExpiresInSeconds) => {
        set({
          isAuthenticated: true,
          token,
          refreshToken,
          tokenExpiresAt: resolveExpiry(expiresInSeconds),
          refreshTokenExpiresAt: resolveExpiry(refreshExpiresInSeconds),
          user,
        })
      },
      updateTokens: (token, refreshToken, expiresInSeconds, refreshExpiresInSeconds) => {
        set((state) => ({
          isAuthenticated: true,
          token,
          refreshToken: refreshToken ?? state.refreshToken,
          tokenExpiresAt: resolveExpiry(expiresInSeconds),
          refreshTokenExpiresAt:
            typeof refreshExpiresInSeconds === 'number'
              ? resolveExpiry(refreshExpiresInSeconds)
              : state.refreshTokenExpiresAt,
        }))
      },
      logout: () => {
        set({
          isAuthenticated: false,
          token: null,
          refreshToken: null,
          tokenExpiresAt: null,
          refreshTokenExpiresAt: null,
          user: null,
        })
      },
      updateUser: (userData) => {
        set((state) => ({
          user: state.user ? { ...state.user, ...userData } : null,
        }))
      },
    }),
    {
      name: 'auth-storage',
      partialize: (state) => ({
        isAuthenticated: state.isAuthenticated,
        token: state.token,
        refreshToken: state.refreshToken,
        tokenExpiresAt: state.tokenExpiresAt,
        refreshTokenExpiresAt: state.refreshTokenExpiresAt,
        user: state.user,
      }),
    }
  )
)
