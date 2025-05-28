import tensorflow as tf

class VARNN(tf.keras.Model):
    def __init__(self, var_weights, var_bias, hidden_units, p, no_columns):
        super(VARNN, self).__init__()
        self.ffnn_model = tf.keras.Sequential([
            tf.keras.layers.Flatten(input_shape=(p, no_columns)),
            tf.keras.layers.Dense(hidden_units, activation='sigmoid')
        ])
        self.var_weights = tf.cast(var_weights, tf.float32)
        self.var_bias = tf.cast(var_bias, tf.float32)
        self.hidden_units = hidden_units

    def call(self, inputs):
        ffnn_output = self.ffnn_model(inputs)
        ffnn_output = tf.reshape(ffnn_output, [-1, self.hidden_units])
        var_output = tf.matmul(ffnn_output, self.var_weights) + self.var_bias
        return var_output