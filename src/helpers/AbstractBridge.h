#pragma once

#include <Mesh.h>

class AbstractBridge {
public:
  virtual ~AbstractBridge() {}

  /**
   * @brief Initializes the bridge.
   */
  virtual void begin() = 0;

  /**
   * @brief Stops the bridge.
   */
  virtual void end() = 0;

  /**
   * @brief Gets the current state of the bridge.
   *
   * @return true if the bridge is initialized and running, false otherwise.
   */
  virtual bool isRunning() const = 0;

  /**
   * @brief A method to be called on every main loop iteration.
   *        Used for tasks like checking for incoming data.
   */
  virtual void loop() = 0;

  /**
   * @brief A callback that is triggered when the mesh transmits a packet.
   *        The bridge can use this to forward the packet.
   *
   * @param packet The packet that was transmitted.
   */
  virtual void sendPacket(mesh::Packet* packet) = 0;

  /**
   * @brief Processes a received packet from the bridge's medium.
   *
   * @param packet The packet that was received.
   */
  virtual void onPacketReceived(mesh::Packet* packet) = 0;

  /**
   * @brief Called for every packet received from the radio, before the repeater logic runs.
   * Default: ignored. Used by bridges that need to see all radio traffic (e.g. MQTT).
   *
   * @param packet The packet that was received.
   */
  virtual void onRadioRx(mesh::Packet* packet) { (void)packet; }

  /**
   * @brief Called for every packet this node transmits on the radio.
   * Default: ignored.
   *
   * @param packet The packet that was transmitted.
   */
  virtual void onRadioTx(mesh::Packet* packet) { (void)packet; }

  /**
   * @brief Tells the bridge this node's public key (its hash is the first byte).
   * Default: ignored.
   *
   * @param pub_key Public key bytes.
   * @param len Number of bytes in pub_key.
   */
  virtual void setSelfIdentity(const uint8_t* pub_key, size_t len) { (void)pub_key; (void)len; }
};
