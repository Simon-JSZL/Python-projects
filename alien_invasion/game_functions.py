import sys
import pygame
from bullet import Bullet
from alien import Alien


def check_events(ai_setting, screen, ship, bullets):
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            sys.exit()

        elif event.type == pygame.KEYDOWN:
            check_keydown_events(event, ai_setting, screen, ship, bullets)

        elif event.type == pygame.KEYUP:
            check_keyup_events(event, ship)


def check_keydown_events(event, ai_setting, screen, ship, bullets):
    if event.key == pygame.K_RIGHT:
        ship.moving_right = True
    elif event.key == pygame.K_LEFT:
        ship.moving_left = True
    elif event.key == pygame.K_SPACE:
        if len(bullets) < ai_setting.bullets_allowed:
            new_bullet = Bullet(ai_setting, screen, ship)
            bullets.add(new_bullet)


def check_keyup_events(event, ship):
    if event.key == pygame.K_RIGHT:
        ship.moving_right = False
    elif event.key == pygame.K_LEFT:
        ship.moving_left = False


def create_fleet(ai_setting, screen, aliens):
    alien = Alien(ai_setting, screen)
    alien_width = alien.rect.width
    number_aliens_x = get_number_aliens_x(ai_setting, alien_width)

    for alien_number in range(number_aliens_x):
        create_aliens(ai_setting, screen, alien_width, alien_number, aliens)


def get_number_aliens_x(ai_setting, alien_width):
    available_space_x = ai_setting.screen_width - 2 * alien_width
    number_alien_x = int(available_space_x / (2 * alien_width))
    return number_alien_x


def create_aliens(ai_setting, screen, alien_width, alien_number, aliens):
    alien = Alien(ai_setting, screen)
    alien.x = alien_width + 2 * alien_width * alien_number
    alien.rect.x = alien.x
    aliens.add(alien)


def update_screen(ai_setting, screen, ship, bullets, aliens):
    screen.fill(ai_setting.bg_color)
    ship.blitme()
    aliens.draw(screen)
    for bullet in bullets.sprites():
        if bullet.rect.bottom < 0:
            bullets.remove(bullet)
        else:
            bullet.draw_bullet()

    pygame.display.flip()
