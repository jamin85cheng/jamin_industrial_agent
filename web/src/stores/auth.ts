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
  user: User | null
  login: (token: string, user: User) => void
  logout: () => void
  updateUser: (user: Partial<User>) => void
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      isAuthenticated: false,
      token: null,
      user: null,
      login: (token, user) => {
        set({
          isAuthenticated: true,
          token,
          user,
        })
      },
      logout: () => {
        set({
          isAuthenticated: false,
          token: null,
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
        user: state.user,
      }),
    }
  )
)
