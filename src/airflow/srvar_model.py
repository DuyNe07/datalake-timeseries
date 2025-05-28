import torch
import torch.nn as nn
import torch.optim as optim

class LSTMEncoder(nn.Module):
    def __init__(self, input_dim, hidden_dim):
        super(LSTMEncoder, self).__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)

    def forward(self, x):
        _, (h_n, _) = self.lstm(x)
        return h_n[-1]

class StateRegularizedUnit(nn.Module):
    def __init__(self, hidden_dim, num_states, tau=1.0):
        super(StateRegularizedUnit, self).__init__()
        self.state_encodings = nn.Parameter(torch.randn(num_states, hidden_dim))
        self.tau = tau

    def forward(self, u_t):
        proximity_scores = torch.matmul(u_t, self.state_encodings.T)
        alpha = torch.softmax(proximity_scores / self.tau, dim=-1)
        h_t = torch.matmul(alpha, self.state_encodings)
        entropy_reg = -torch.sum(alpha * torch.log(alpha + 1e-10), dim=-1).mean()
        return h_t, alpha, entropy_reg

class GraphGeneration(nn.Module):
    def __init__(self, hidden_dim, num_vars):
        super(GraphGeneration, self).__init__()
        self.num_vars = num_vars
        self.g_a = nn.Linear(hidden_dim, num_vars * num_vars)
        self.g_d = nn.Linear(hidden_dim, num_vars)

    def forward(self, c_st):
        A_st = self.g_a(c_st).view(-1, self.num_vars, self.num_vars)
        D_st = torch.diag_embed(self.g_d(c_st))
        exp_A = torch.matrix_exp(A_st * A_st)
        trace_exp_A = torch.sum(torch.diagonal(exp_A, dim1=-2, dim2=-1), dim=-1)
        acyclic_penalty = torch.mean(trace_exp_A - A_st.size(-1))
        return A_st, D_st, acyclic_penalty

class DynamicVAR(nn.Module):
    def __init__(self, num_vars):
        super(DynamicVAR, self).__init__()
        self.var_weights = nn.Parameter(torch.randn(num_vars, num_vars))

    def forward(self, x_t, A_st, D_st):
        x_t = x_t.unsqueeze(-1)
        x_hat_A = torch.matmul(A_st, x_t).squeeze(-1)
        weighted_x_t = torch.matmul(self.var_weights, x_t.squeeze(-1).unsqueeze(-1)).squeeze(-1)
        x_hat_D = torch.matmul(D_st, weighted_x_t.unsqueeze(-1)).squeeze(-1)
        return x_hat_A + x_hat_D

class SrVAR(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_states, num_vars, tau=1.0):
        super(SrVAR, self).__init__()
        self.encoder = LSTMEncoder(input_dim, hidden_dim)
        self.state_regularized_unit = StateRegularizedUnit(hidden_dim, num_states, tau)
        self.graph_generation = GraphGeneration(hidden_dim, num_vars)
        self.dynamic_var = DynamicVAR(num_vars)

    def forward(self, x):
        u_t = self.encoder(x)
        h_t, alpha, entropy_reg = self.state_regularized_unit(u_t)
        A_st, D_st, acyclic_penalty = self.graph_generation(h_t)
        x_hat = self.dynamic_var(x[:, -1, :], A_st, D_st)
        return x_hat, alpha, entropy_reg, acyclic_penalty