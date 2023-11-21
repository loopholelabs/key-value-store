/*
	Copyright 2023 Loophole Labs

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		   http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package badger

import "github.com/rs/zerolog"

// HTTP is the logger object that can be embedded in the HTTP package
type Logger struct {
	// Logger inherits from the zerolog.Logger type
	*zerolog.Logger
}

// Errorf logs an Error level message
func (l *Logger) Errorf(s string, i ...interface{}) {
	l.Error().Msgf(s, i...)
}

// Warningf logs a Warn level message
func (l *Logger) Warningf(s string, i ...interface{}) {
	l.Warn().Msgf(s, i...)
}

// Infof logs an Info level message
func (l *Logger) Infof(s string, i ...interface{}) {
	l.Info().Msgf(s, i...)
}

// Debugf logs a Debug level message
func (l *Logger) Debugf(s string, i ...interface{}) {
	l.Debug().Msgf(s, i...)
}
